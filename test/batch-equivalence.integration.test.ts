import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { type BetEvent, EventKind } from '../src/contracts';
import { parseConfig } from '../src/config';
import { DatabaseService } from '../src/db';
import { PermanentEventError } from '../src/tournament/errors';
import { BetEventsService } from '../src/tournament/services';
import type { BetEventInput } from '../src/tournament/services/bet-batch';
import { storeOneByOne } from './support/reference-storage';

const connectionString = process.env.TEST_DATABASE_URL;

/** Deterministic PRNG so a failure can be replayed. */
function random(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

interface Scenario {
  events: BetEventInput[];
}

/**
 * Random events over a small set of bets and players: repeats, equivalent repeats
 * (other decimal form or timezone), conflicts, foreign players, rollbacks before and
 * after settlement, zero wins and an unsupported currency.
 */
function scenario(
  seed: number,
  size: number,
  withConflicts: boolean,
): Scenario {
  const next = random(seed);
  const pick = <T>(items: readonly T[]) =>
    items.at(Math.floor(next() * items.length))!;
  const players = Array.from({ length: 12 }, (_, index) => `player-${index}`);
  const bets = Array.from({ length: 90 }, (_, index) => {
    const second = String(Math.floor(next() * 60)).padStart(2, '0');
    const micro = String(Math.floor(next() * 1_000_000)).padStart(6, '0');
    return {
      betId: `bet-${index}`,
      playerId: pick(players),
      currency: index % 29 === 0 ? 'GBP' : pick(['USD', 'EUR', 'RUB']),
      stake: '5',
      winAmount: pick(['0', '1', '2.5', '10', '0.000001']),
      settledAt: `2026-11-18T12:${second}:00.${micro}Z`,
      offset: `2026-11-18T15:${second}:00.${micro}+03:00`,
    };
  });
  const events: BetEventInput[] = [];
  for (let index = 0; index < size; index += 1) {
    const bet = pick(bets);
    const base: BetEvent = {
      betId: bet.betId,
      playerId: bet.playerId,
      currency: bet.currency,
      stake: bet.stake,
      winAmount: bet.winAmount,
      settledAt: bet.settledAt,
    };
    const roll = next();
    if (roll < 0.25) {
      events.push({ kind: EventKind.BetRollback, event: base });
    } else if (roll < 0.35) {
      // Same instant in another timezone, same amount in another decimal form.
      events.push({
        kind: EventKind.BetSettled,
        event: {
          ...base,
          settledAt: bet.offset,
          // Another decimal form of the same amount, within six fractional digits.
          winAmount: !bet.winAmount.includes('.')
            ? `${bet.winAmount}.0`
            : bet.winAmount.split('.')[1]!.length < 6
              ? `${bet.winAmount}0`
              : bet.winAmount,
        },
      });
    } else if (withConflicts && roll < 0.4) {
      events.push({
        kind: EventKind.BetSettled,
        event: { ...base, winAmount: '999' },
      });
    } else if (withConflicts && roll < 0.43) {
      events.push({
        kind: EventKind.BetSettled,
        event: {
          ...base,
          settledAt: base.settledAt.replace(/\d{6}Z$/, '000000Z'),
        },
      });
    } else if (withConflicts && roll < 0.46) {
      events.push({
        kind: next() < 0.5 ? EventKind.BetSettled : EventKind.BetRollback,
        event: { ...base, playerId: 'intruder' },
      });
    } else {
      events.push({ kind: EventKind.BetSettled, event: base });
    }
  }
  return { events };
}

const describeOutcome = (outcome: unknown) =>
  outcome instanceof PermanentEventError ? `dlq: ${outcome.message}` : outcome;

describe.skipIf(!connectionString)(
  'batched storage equals the former one-event-per-transaction storage',
  () => {
    const schemas = {
      single: `test_${randomUUID().replaceAll('-', '')}`,
      batch: `test_${randomUUID().replaceAll('-', '')}`,
    };
    let admin: Pool;
    const databases: DatabaseService[] = [];
    type Store = {
      db: DatabaseService;
      events: BetEventsService;
      one: (kind: EventKind, event: BetEvent) => Promise<unknown>;
    };
    let single: Store;
    let batch: Store;

    async function open(schema: string) {
      await admin.query(`CREATE SCHEMA "${schema}"`);
      const url = new URL(connectionString!);
      url.searchParams.set('options', `-c search_path=${schema}`);
      const config = parseConfig({
        DATABASE_URL: url.toString(),
        RABBITMQ_URL: 'amqp://localhost',
        EXCHANGE_RATES: 'USD=1;EUR=1.1;RUB=0.01',
        LOG_LEVEL: 'silent',
        DATABASE_MAX_POOL: '8',
      });
      const db = new DatabaseService(config);
      databases.push(db);
      await migrate(db.db, {
        migrationsFolder: fileURLToPath(
          new URL('../drizzle/migrations', import.meta.url),
        ),
        migrationsSchema: schema,
      });
      return {
        db,
        events: new BetEventsService(db, config),
        one: (kind: EventKind, event: BetEvent) =>
          storeOneByOne(db, config.exchangeRates, kind, event),
      };
    }

    async function state(db: DatabaseService, scoredOnly = false) {
      // A tombstone keeps the settlement only when the rollback came second, so the
      // order-independent comparison reads state and contribution alone.
      const bets = await db.pool.query(
        scoredOnly
          ? `SELECT bet_id, player_id, state, contribution::text FROM bets ORDER BY bet_id`
          : `SELECT bet_id, player_id, state, settlement, settled_at::text, contribution::text,
               exchange_rate::text FROM bets ORDER BY bet_id`,
      );
      const scores = await db.pool.query(
        `SELECT player_id, points::text, score_reached_at::text FROM player_scores
         ${scoredOnly ? 'WHERE points > 0' : ''} ORDER BY player_id`,
      );
      return { bets: bets.rows, scores: scores.rows };
    }

    beforeAll(async () => {
      admin = new Pool({ connectionString });
      single = await open(schemas.single);
      batch = await open(schemas.batch);
    });

    beforeEach(async () => {
      for (const db of databases) {
        await db.pool.query('TRUNCATE bets, player_scores');
      }
    });

    afterAll(async () => {
      for (const db of databases) await db.onApplicationShutdown();
      for (const schema of Object.values(schemas)) {
        await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      }
      await admin.end();
    });

    it.each([1, 2, 3, 4, 5, 6])(
      'gives the same outcomes and stored state for random sequence %i',
      async (seed) => {
        const { events } = scenario(seed, 500, true);
        const expected: unknown[] = [];
        for (const { kind, event } of events) {
          expected.push(
            await single.one(kind, event).catch((error: unknown) => error),
          );
        }

        const next = random(seed * 7919);
        const actual: unknown[] = [];
        for (let start = 0; start < events.length;) {
          const size = 1 + Math.floor(next() * 40);
          actual.push(
            ...(await batch.events.handleBatch(
              events.slice(start, start + size),
            )),
          );
          start += size;
        }

        expect(actual.map(describeOutcome)).toEqual(
          expected.map(describeOutcome),
        );
        expect(await state(batch.db)).toEqual(await state(single.db));
      },
    );

    it('stays deadlock-free and order-independent with concurrent overlapping batches', async () => {
      const { events } = scenario(42, 800, false);
      for (const { kind, event } of events) {
        await single.one(kind, event).catch(() => undefined);
      }
      // Eight concurrent streams over the same players and bets, in shuffled order.
      const next = random(4242);
      const shuffled = [...events].sort(() => next() - 0.5);
      const streams = Array.from({ length: 8 }, (_, stream) =>
        shuffled.filter((_event, index) => index % 8 === stream),
      );
      await Promise.all(
        streams.map(async (stream) => {
          for (let start = 0; start < stream.length; start += 25) {
            await batch.events.handleBatch(stream.slice(start, start + 25));
          }
        }),
      );
      // Rollback is final and duplicates add nothing, so balances do not depend on
      // order; whether a zero-balance participant row exists does.
      expect(await state(batch.db, true)).toEqual(await state(single.db, true));
    });
  },
);
