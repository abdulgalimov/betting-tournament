import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import type { BetEvent } from '../src/contracts';
import { parseConfig, type AppConfig } from '../src/config';
import { DatabaseService } from '../src/db';
import {
  BetEventsService,
  LeaderboardService,
} from '../src/tournament/services';
import { PermanentEventError } from '../src/tournament/errors';
import { BetConsumer } from '../src/tournament/messaging';

const connectionString = process.env.TEST_DATABASE_URL;

describe.skipIf(!connectionString)('PostgreSQL tournament storage', () => {
  const schema = `test_${randomUUID().replaceAll('-', '')}`;
  let admin: Pool;
  let database: DatabaseService;
  let events: BetEventsService;
  let leaderboardService: LeaderboardService;
  let config: AppConfig;

  const event = (overrides: Partial<BetEvent> = {}): BetEvent => ({
    betId: randomUUID(),
    playerId: 'alice',
    currency: 'USD',
    stake: '10',
    winAmount: '20',
    settledAt: '2026-11-18T12:00:00.000Z',
    ...overrides,
  });

  const scoreReachedAt = async (playerId: string): Promise<string | null> => {
    const result = await database.pool.query<{ reachedAt: string | null }>(
      `SELECT to_char(score_reached_at AT TIME ZONE 'UTC',
        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "reachedAt"
       FROM player_scores WHERE player_id = $1`,
      [playerId],
    );
    return result.rows[0]!.reachedAt;
  };

  beforeAll(async () => {
    admin = new Pool({ connectionString });
    await admin.query(`CREATE SCHEMA "${schema}"`);
    const isolatedUrl = new URL(connectionString!);
    isolatedUrl.searchParams.set('options', `-c search_path=${schema}`);
    config = parseConfig({
      DATABASE_URL: isolatedUrl.toString(),
      RABBITMQ_URL: 'amqp://localhost',
      EXCHANGE_RATES: 'USD=1;EUR=1.1;RUB=0.01',
      LOG_LEVEL: 'silent',
    });
    database = new DatabaseService(config);
    await migrate(database.db, {
      migrationsFolder: fileURLToPath(
        new URL('../drizzle/migrations', import.meta.url),
      ),
      migrationsSchema: schema,
    });
    events = new BetEventsService(database, config);
    leaderboardService = new LeaderboardService(database);
  });

  beforeEach(async () => {
    await database.pool.query('TRUNCATE bets, player_scores');
    config.exchangeRates.EUR = '1.1';
  });

  afterAll(async () => {
    await database?.onApplicationShutdown();
    if (admin) {
      await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      await admin.end();
    }
  });

  it('credits full winAmount, converts currency, and returns exact point strings', async () => {
    await events.handleEvent(
      'bet.settled',
      event({ currency: 'EUR', winAmount: '0.1', stake: '100' }),
    );
    expect(await leaderboardService.leaderboard('alice')).toEqual({
      top: [{ playerId: 'alice', points: '0.11', position: 1 }],
      player: { playerId: 'alice', points: '0.11', position: 1 },
    });
  });

  it('credits concurrent duplicate deliveries only once', async () => {
    const bet = event();
    const results = await Promise.all(
      Array.from({ length: 15 }, () => events.handleEvent('bet.settled', bet)),
    );
    expect(results.filter((result) => result === 'settled')).toHaveLength(1);
    expect((await leaderboardService.leaderboard('alice')).player?.points).toBe(
      '20',
    );
  });

  it('keeps large decimal amounts exact through PostgreSQL and JSON aggregation', async () => {
    await events.handleEvent(
      'bet.settled',
      event({ currency: 'EUR', winAmount: '9007199254740993.123456' }),
    );
    expect((await leaderboardService.leaderboard('alice')).player?.points).toBe(
      '9907919180215092.4358016',
    );
  });

  it('does not lose updates from independent concurrent bets for one player', async () => {
    await Promise.all(
      Array.from({ length: 20 }, () =>
        events.handleEvent('bet.settled', event({ winAmount: '0.01' })),
      ),
    );
    expect((await leaderboardService.leaderboard('alice')).player?.points).toBe(
      '0.2',
    );
  });

  it('retries a real PostgreSQL lock_timeout while continuing to process other deliveries', async () => {
    const consumer = new BetConsumer(config, events);
    Object.defineProperty(consumer, 'logger', {
      value: { error: vi.fn() },
    });
    const channel = { ack: vi.fn(), nack: vi.fn(), reject: vi.fn() };
    const deliver = (message: ConsumeMessage) =>
      consumer.handleDelivery(
        channel as unknown as ConfirmChannel,
        message,
        async () => undefined,
      );
    const message = (bet: BetEvent) =>
      ({
        content: Buffer.from(JSON.stringify(bet)),
        fields: { exchange: 'bets', routingKey: 'bet.settled' },
        properties: {},
      }) as ConsumeMessage;
    const contendedBet = event();
    const contended = message(contendedBet);
    const unrelated = message(event({ playerId: 'bob' }));
    const locker = await database.pool.connect();
    try {
      await locker.query('BEGIN');
      await locker.query(
        'SELECT pg_advisory_xact_lock(hashtextextended($1, 0))',
        [contendedBet.betId],
      );

      await expect(deliver(contended)).resolves.toBe('retryable_error');
      expect(channel.reject).toHaveBeenCalledWith(contended, true);
      expect(channel.nack).not.toHaveBeenCalled();
      await expect(deliver(unrelated)).resolves.toBe('settled');
      expect(channel.ack).toHaveBeenCalledWith(unrelated);
    } finally {
      try {
        await locker.query('ROLLBACK');
      } finally {
        locker.release();
      }
    }

    await expect(deliver(contended)).resolves.toBe('settled');
    expect(channel.ack).toHaveBeenCalledWith(contended);
  });

  it('treats numerically equivalent duplicate amounts and timestamps as identical', async () => {
    const bet = event({ winAmount: '20.0' });
    await events.handleEvent('bet.settled', bet);
    await expect(
      events.handleEvent('bet.settled', {
        ...bet,
        winAmount: '20.000000',
        settledAt: '2026-11-18T15:00:00+03:00',
      }),
    ).resolves.toBe('duplicate');
  });

  it('rejects conflicting duplicate bodies and player identity collisions without mutation', async () => {
    const bet = event();
    await events.handleEvent('bet.settled', bet);
    await expect(
      events.handleEvent('bet.settled', { ...bet, winAmount: '30' }),
    ).rejects.toBeInstanceOf(PermanentEventError);
    await expect(
      events.handleEvent('bet.rollback', { ...bet, playerId: 'bob' }),
    ).rejects.toBeInstanceOf(PermanentEventError);
    expect((await leaderboardService.leaderboard('alice')).player?.points).toBe(
      '20',
    );
    expect((await leaderboardService.leaderboard('bob')).player).toBeNull();
  });

  it('rolls back saved points exactly once even after exchange rates change', async () => {
    const bet = event({ currency: 'EUR' });
    await events.handleEvent('bet.settled', bet);
    config.exchangeRates.EUR = '2';
    await Promise.all(
      Array.from({ length: 10 }, () => events.handleEvent('bet.rollback', bet)),
    );
    expect((await leaderboardService.leaderboard('alice')).player?.points).toBe(
      '0',
    );
    await expect(events.handleEvent('bet.settled', bet)).resolves.toBe(
      'ignored',
    );
  });

  it('persists rollback-before-settlement tombstones', async () => {
    const bet = event();
    await events.handleEvent('bet.rollback', bet);
    await expect(events.handleEvent('bet.settled', bet)).resolves.toBe(
      'ignored',
    );
    expect(await leaderboardService.leaderboard('alice')).toEqual({
      top: [],
      player: null,
    });
  });

  it('settlement/rollback races always leave zero contribution', async () => {
    const bet = event();
    await Promise.all([
      events.handleEvent('bet.settled', bet),
      events.handleEvent('bet.rollback', bet),
      events.handleEvent('bet.settled', bet),
      events.handleEvent('bet.rollback', bet),
    ]);
    expect(
      (await leaderboardService.leaderboard('alice')).player?.points ?? '0',
    ).toBe('0');
    expect(
      (await database.pool.query('SELECT state, contribution FROM bets'))
        .rows[0],
    ).toMatchObject({ state: 'rolled_back', contribution: '0.000000000000' });
  });

  it('rejects unknown currencies without persisting partial state', async () => {
    await expect(
      events.handleEvent('bet.settled', event({ currency: 'XXX' })),
    ).rejects.toBeInstanceOf(PermanentEventError);
    expect(
      (await database.pool.query('SELECT count(*)::integer AS count FROM bets'))
        .rows[0].count,
    ).toBe(0);
    expect((await leaderboardService.leaderboard('alice')).player).toBeNull();
  });

  it('limits top to 100 while returning a player outside top with stable tie order', async () => {
    await database.pool
      .query(`INSERT INTO player_scores (player_id, points, score_reached_at)
      SELECT 'player-' || lpad(number::text, 3, '0'), 1, '2026-11-18T12:00:00Z'::timestamptz
      FROM generate_series(1, 105) number`);
    const leaderboard = await leaderboardService.leaderboard('player-105');
    expect(leaderboard.top).toHaveLength(100);
    expect(leaderboard.top[0]).toEqual({
      playerId: 'player-001',
      points: '1',
      position: 1,
    });
    expect(leaderboard.top[99]).toEqual({
      playerId: 'player-100',
      points: '1',
      position: 100,
    });
    expect(leaderboard.player).toEqual({
      playerId: 'player-105',
      points: '1',
      position: 105,
    });
    expect((await leaderboardService.leaderboard('missing')).player).toBeNull();
  });

  it('ranks equal scores by the earlier achieved instant with microsecond precision', async () => {
    await events.handleEvent(
      'bet.settled',
      event({ playerId: 'a-later', settledAt: '2026-11-18T12:00:00.000002Z' }),
    );
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'z-earlier',
        settledAt: '2026-11-18T12:00:00.000001Z',
      }),
    );
    const leaderboard = await leaderboardService.leaderboard('a-later');
    expect(leaderboard.top.map((entry) => entry.playerId)).toEqual([
      'z-earlier',
      'a-later',
    ]);
    expect(leaderboard.player?.position).toBe(2);
    expect(await scoreReachedAt('z-earlier')).toBe(
      '2026-11-18T12:00:00.000001Z',
    );
    expect(await scoreReachedAt('a-later')).toBe('2026-11-18T12:00:00.000002Z');
  });

  it('uses the latest contributing settlement rather than the first point earned', async () => {
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'z-first-point',
        winAmount: '5',
        settledAt: '2026-11-18T10:00:00Z',
      }),
    );
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'a-first-total',
        winAmount: '10',
        settledAt: '2026-11-18T12:00:00Z',
      }),
    );
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'z-first-point',
        winAmount: '5',
        settledAt: '2026-11-18T13:00:00Z',
      }),
    );
    expect(
      (await leaderboardService.leaderboard('z-first-point')).top.map(
        (entry) => entry.playerId,
      ),
    ).toEqual(['a-first-total', 'z-first-point']);
    expect(await scoreReachedAt('z-first-point')).toBe(
      '2026-11-18T13:00:00.000000Z',
    );
  });

  it('uses event time for late deliveries and never moves the achievement time backwards', async () => {
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'a-delivered-first',
        winAmount: '5',
        settledAt: '2026-11-18T12:00:00Z',
      }),
    );
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'z-earlier-total',
        winAmount: '10',
        settledAt: '2026-11-18T11:00:00Z',
      }),
    );
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'a-delivered-first',
        winAmount: '5',
        settledAt: '2026-11-18T10:00:00Z',
      }),
    );
    expect(
      (await leaderboardService.leaderboard('a-delivered-first')).player
        ?.position,
    ).toBe(2);
    expect(await scoreReachedAt('a-delivered-first')).toBe(
      '2026-11-18T12:00:00.000000Z',
    );
  });

  it('falls back to playerId for the same instant and ignores later zero-win bets', async () => {
    await events.handleEvent(
      'bet.settled',
      event({ playerId: 'z-player', settledAt: '2026-11-18T12:00:00.123456Z' }),
    );
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'a-player',
        settledAt: '2026-11-18T15:00:00.123456+03:00',
      }),
    );
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'a-player',
        winAmount: '0',
        settledAt: '2026-11-18T23:00:00Z',
      }),
    );
    expect(
      (await leaderboardService.leaderboard('a-player')).top.map(
        (entry) => entry.playerId,
      ),
    ).toEqual(['a-player', 'z-player']);
    expect(await scoreReachedAt('a-player')).toBe(
      '2026-11-18T12:00:00.123456Z',
    );
  });

  it('preserves achievement time for equivalent duplicates and rejects changed microseconds', async () => {
    const bet = event({
      winAmount: '20.0',
      settledAt: '2026-11-18T12:00:00.000001Z',
    });
    await events.handleEvent('bet.settled', bet);
    await expect(
      events.handleEvent('bet.settled', {
        ...bet,
        winAmount: '20.000000',
        settledAt: '2026-11-18T15:00:00.000001+03:00',
      }),
    ).resolves.toBe('duplicate');
    await expect(
      events.handleEvent('bet.settled', {
        ...bet,
        settledAt: '2026-11-18T12:00:00.000002Z',
      }),
    ).rejects.toBeInstanceOf(PermanentEventError);
    expect(await scoreReachedAt('alice')).toBe('2026-11-18T12:00:00.000001Z');
    expect((await leaderboardService.leaderboard('alice')).player?.points).toBe(
      '20',
    );
  });

  it('recalculates the surviving achievement time after rollback and clears it at zero', async () => {
    const older = event({
      playerId: 'z-player',
      winAmount: '5',
      settledAt: '2026-11-18T10:00:00Z',
    });
    const latest = event({
      playerId: 'z-player',
      winAmount: '5',
      settledAt: '2026-11-18T13:00:00Z',
    });
    await events.handleEvent('bet.settled', older);
    await events.handleEvent('bet.settled', latest);
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'a-player',
        winAmount: '5',
        settledAt: '2026-11-18T12:00:00Z',
      }),
    );
    await events.handleEvent('bet.rollback', latest);
    expect((await leaderboardService.leaderboard('z-player')).player).toEqual({
      playerId: 'z-player',
      points: '5',
      position: 1,
    });
    expect(await scoreReachedAt('z-player')).toBe(
      '2026-11-18T10:00:00.000000Z',
    );
    await events.handleEvent('bet.rollback', older);
    expect((await leaderboardService.leaderboard('z-player')).player).toEqual({
      playerId: 'z-player',
      points: '0',
      position: 2,
    });
    expect(await scoreReachedAt('z-player')).toBeNull();
  });

  it('keeps zero-score achievement times null and resolves their ties by playerId', async () => {
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'z-player',
        winAmount: '0',
        settledAt: '2026-11-18T10:00:00Z',
      }),
    );
    await events.handleEvent(
      'bet.settled',
      event({
        playerId: 'a-player',
        winAmount: '0',
        settledAt: '2026-11-18T13:00:00Z',
      }),
    );
    expect(
      (await leaderboardService.leaderboard('z-player')).top.map(
        (entry) => entry.playerId,
      ),
    ).toEqual(['a-player', 'z-player']);
    expect(await scoreReachedAt('z-player')).toBeNull();
    expect(await scoreReachedAt('a-player')).toBeNull();
  });

  it('serializes different-bet settlements and rollbacks for one player without a stale maximum', async () => {
    const cancelled = event({
      settledAt: '2026-11-18T23:00:00Z',
      winAmount: '100',
    });
    await events.handleEvent('bet.settled', cancelled);
    await Promise.all([
      events.handleEvent('bet.rollback', cancelled),
      ...Array.from({ length: 12 }, (_, index) =>
        events.handleEvent(
          'bet.settled',
          event({
            winAmount: '1',
            settledAt: `2026-11-18T12:00:${String(index).padStart(2, '0')}.000001Z`,
          }),
        ),
      ),
    ]);
    expect((await leaderboardService.leaderboard('alice')).player?.points).toBe(
      '12',
    );
    expect(await scoreReachedAt('alice')).toBe('2026-11-18T12:00:11.000001Z');
  });

  it('uses achievement time consistently for a personal position beyond the top 100', async () => {
    await database.pool
      .query(`INSERT INTO player_scores (player_id, points, score_reached_at)
      SELECT 'player-' || lpad(number::text, 3, '0'), 1,
        '2026-11-18T12:00:00Z'::timestamptz + (106 - number) * interval '1 second'
      FROM generate_series(1, 105) number`);
    const leaderboard = await leaderboardService.leaderboard('player-001');
    expect(leaderboard.top).toHaveLength(100);
    expect(leaderboard.top[0]?.playerId).toBe('player-105');
    expect(leaderboard.top[99]?.playerId).toBe('player-006');
    expect(leaderboard.player).toEqual({
      playerId: 'player-001',
      points: '1',
      position: 105,
    });
  });

  it('counts personal positions exactly as the documented order for every player', async () => {
    // Few distinct points and instants, NULL times at zero, mixed-case ids for COLLATE "C".
    await database.pool
      .query(`INSERT INTO player_scores (player_id, points, score_reached_at)
      SELECT (CASE WHEN n % 3 = 0 THEN 'P' ELSE 'p' END) || '-' || (n * 7919 % 1000),
        points,
        CASE WHEN points = 0 THEN NULL
          ELSE '2026-11-18T12:00:00Z'::timestamptz + (n % 4) * interval '1 microsecond' END
      FROM (SELECT n, (ARRAY[0, 1, 1.5, 2, 1000000000000000000.000000000001])[1 + n % 5]::numeric AS points
        FROM generate_series(1, 400) n) source
      ON CONFLICT DO NOTHING`);
    const expected = await database.pool.query<{
      playerId: string;
      position: number;
    }>(`SELECT player_id AS "playerId", (row_number() OVER (
        ORDER BY points DESC, score_reached_at ASC NULLS LAST, player_id COLLATE "C" ASC))::integer AS position
      FROM player_scores`);
    expect(expected.rows.length).toBeGreaterThan(300);
    const mismatches: string[] = [];
    for (const { playerId, position } of expected.rows) {
      const actual = (await leaderboardService.leaderboard(playerId)).player
        ?.position;
      if (actual !== position) {
        mismatches.push(`${playerId}: ${actual} != ${position}`);
      }
    }
    expect(mismatches).toEqual([]);
  });

  it('applies separate timeouts to the HTTP pool and the event pool', async () => {
    const setting = async (
      pool: typeof database.pool,
      name: string,
    ): Promise<string> =>
      (await pool.query<Record<string, string>>(`SHOW ${name}`)).rows[0]![
        name
      ]!;
    expect(await setting(database.readerPool, 'statement_timeout')).toBe('2s');
    expect(await setting(database.pool, 'statement_timeout')).toBe('10s');
    expect(await setting(database.pool, 'lock_timeout')).toBe('5s');
    expect(
      await setting(database.pool, 'idle_in_transaction_session_timeout'),
    ).toBe('30s');
    expect(database.readerPool.options.max).toBe(8);
    expect(database.pool.options.max).toBe(8);
  });

  it('tunes autovacuum for player_scores through migration 0002', async () => {
    const result = await database.pool.query<{ options: string[] }>(
      `SELECT reloptions AS options FROM pg_class
       WHERE oid = 'player_scores'::regclass`,
    );
    expect(result.rows[0]!.options).toEqual(
      expect.arrayContaining([
        'autovacuum_vacuum_scale_factor=0.01',
        'autovacuum_vacuum_insert_scale_factor=0.01',
        'autovacuum_vacuum_cost_limit=2000',
      ]),
    );
  });

  it('protects nonnegative persisted scores using a database constraint', async () => {
    await expect(
      database.pool.query(
        "INSERT INTO player_scores (player_id, points) VALUES ('alice', -1)",
      ),
    ).rejects.toMatchObject({ code: '23514' });
  });
});
