import { DrizzleQueryError } from 'drizzle-orm';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import type { BetEvent } from '../src/contracts';
import { BetConsumer } from '../src/tournament/messaging';
import { PermanentEventError } from '../src/tournament/errors';
import { AppConfig, parseConfig } from '../src/config';
import type { BetEventInput } from '../src/tournament/services/bet-batch';
import { batchOf } from './support/batch-service';

const bet: BetEvent = {
  betId: 'bet-1',
  playerId: 'alice',
  currency: 'USD',
  stake: '10',
  winAmount: '25',
  settledAt: '2026-11-18T12:00:00Z',
};
const delivery = (body: unknown = bet, routingKey = 'bet.settled') =>
  ({
    content: Buffer.from(JSON.stringify(body)),
    fields: { routingKey },
    properties: {},
  }) as ConsumeMessage;
const setup = (config: AppConfig = new AppConfig()) => {
  const channel = { ack: vi.fn(), nack: vi.fn(), reject: vi.fn() };
  const logger = { error: vi.fn(), warn: vi.fn() };
  const retry = vi.fn(async (_previousDeliveries: number) => undefined);
  const service = {
    handleEvent: vi.fn(
      async (_kind: string, _event: BetEvent): Promise<string> => 'settled',
    ),
    handleBatch: vi.fn(),
    checkDatabase: vi.fn(async () => undefined),
  };
  service.handleBatch.mockImplementation(batchOf(service.handleEvent));
  const consumer = new BetConsumer(config, service as never);
  Object.defineProperty(consumer, 'logger', { value: logger });
  return {
    channel: channel as typeof channel & ConfirmChannel,
    logger,
    retry,
    consumer,
    service,
  };
};

describe('AMQP delivery boundary', () => {
  it('acknowledges only after the transaction resolves', async () => {
    const context = setup();
    let commit!: () => void;
    const transaction = new Promise<void>((resolve) => {
      commit = resolve;
    });
    context.service.handleEvent.mockImplementation(async () => {
      await transaction;
      return 'settled' as const;
    });
    const message = delivery();
    const processing = context.consumer.handleDelivery(
      context.channel,
      message,
      context.retry,
    );
    await Promise.resolve();
    expect(context.channel.ack).not.toHaveBeenCalled();
    commit();
    await processing;
    expect(context.channel.ack).toHaveBeenCalledWith(message);
    expect(context.channel.reject).not.toHaveBeenCalled();
    expect(context.logger.error).not.toHaveBeenCalled();
  });

  it.each([
    [
      { ...bet, winAmount: 25 },
      'bet.settled',
      'bet.settled',
      'invalid_contract',
    ],
    [
      { ...bet, privateValue: 'sensitive upstream field' },
      'bet.settled',
      'bet.settled',
      'invalid_contract',
    ],
    [bet, 'unexpected.routing.key', undefined, 'invalid_event_kind'],
  ] as const)(
    'dead-letters invalid deliveries and logs only safe metadata',
    async (body, routingKey, kind, reason) => {
      const context = setup();
      const message = delivery(body, routingKey);
      await context.consumer.handleDelivery(
        context.channel,
        message,
        context.retry,
      );
      expect(context.service.handleEvent).not.toHaveBeenCalled();
      expect(context.channel.nack).toHaveBeenCalledWith(message, false, false);
      expect(context.retry).not.toHaveBeenCalled();
      expect(context.logger.error.mock.calls).toEqual([
        [
          {
            kind,
            bodyBytes: message.content.length,
            previousDeliveries: 0,
            reason,
          },
          'Event rejected',
        ],
      ]);
    },
  );

  it('uses the event type only when redrive addresses the configured queue directly', async () => {
    const config = new AppConfig();
    const context = setup(config);
    const direct = delivery(bet, config.rabbitmq.queue);
    direct.fields.exchange = '';
    direct.properties.type = 'bet.rollback';
    const published = delivery();
    published.fields.exchange = config.rabbitmq.exchange;
    published.properties.type = 'bet.rollback';

    await context.consumer.handleDelivery(context.channel, direct);
    await context.consumer.handleDelivery(context.channel, published);

    expect(context.service.handleEvent.mock.calls).toEqual([
      ['bet.rollback', bet],
      ['bet.settled', bet],
    ]);
    expect(context.channel.ack.mock.calls).toEqual([[direct], [published]]);
  });

  it.each([undefined, 'unexpected.private.type'])(
    'dead-letters direct deliveries with a missing or unknown event type',
    async (type) => {
      const config = new AppConfig();
      const context = setup(config);
      const direct = delivery(bet, config.rabbitmq.queue);
      direct.fields.exchange = '';
      if (type !== undefined) direct.properties.type = type;
      direct.properties.headers = {
        'x-delivery-count': { valueOf: 'invalid', toString: 'invalid' },
      };

      await context.consumer.handleDelivery(context.channel, direct);

      expect(context.channel.nack).toHaveBeenCalledWith(direct, false, false);
      expect(context.service.handleEvent).not.toHaveBeenCalled();
      expect(context.logger.error.mock.calls).toEqual([
        [
          {
            kind: undefined,
            bodyBytes: direct.content.length,
            previousDeliveries: 0,
            reason: 'invalid_event_kind',
          },
          'Event rejected',
        ],
      ]);
    },
  );

  it('dead-letters a permanent domain conflict', async () => {
    const context = setup();
    context.service.handleEvent.mockRejectedValue(
      new PermanentEventError(`Conflicting betId ${bet.betId}`),
    );
    const message = delivery();
    await context.consumer.handleDelivery(
      context.channel,
      message,
      context.retry,
    );
    expect(context.channel.nack).toHaveBeenCalledWith(message, false, false);
    expect(context.channel.ack).not.toHaveBeenCalled();
    expect(context.logger.error.mock.calls).toEqual([
      [
        {
          kind: 'bet.settled',
          bodyBytes: message.content.length,
          previousDeliveries: 0,
          reason: 'domain_error',
        },
        'Event rejected',
      ],
    ]);
  });

  it('backs off and rejects transient errors so modern quorum delivery-limit applies', async () => {
    const context = setup();
    context.service.handleEvent.mockRejectedValue(
      new Error('sensitive database details'),
    );
    const message = delivery();
    await context.consumer.handleDelivery(
      context.channel,
      message,
      context.retry,
    );
    expect(context.retry).toHaveBeenCalledOnce();
    expect(context.channel.reject).toHaveBeenCalledWith(message, true);
    expect(context.channel.nack).not.toHaveBeenCalled();
    expect(context.channel.ack).not.toHaveBeenCalled();
    expect(context.logger.error.mock.calls).toEqual([
      [
        {
          kind: 'bet.settled',
          bodyBytes: message.content.length,
          previousDeliveries: 0,
          reason: 'unknown',
        },
        'Event transaction failed; retrying through broker delivery limit',
      ],
    ]);
    expect(JSON.stringify(context.logger.error.mock.calls)).not.toContain(
      'sensitive database details',
    );
  });

  it('rejects malformed JSON without a retry', async () => {
    const context = setup();
    const message = { ...delivery(), content: Buffer.from('{broken') };
    await context.consumer.handleDelivery(
      context.channel,
      message,
      context.retry,
    );
    expect(context.channel.nack).toHaveBeenCalledWith(message, false, false);
    expect(context.logger.error.mock.calls).toEqual([
      [
        {
          kind: 'bet.settled',
          bodyBytes: message.content.length,
          previousDeliveries: 0,
          reason: 'invalid_json',
        },
        'Event rejected',
      ],
    ]);
  });

  it('dead-letters an oversized body and logs only its size and event metadata', async () => {
    const context = setup();
    const message = { ...delivery(), content: Buffer.alloc(16_385, 'x') };

    await context.consumer.handleDelivery(context.channel, message);

    expect(context.channel.nack).toHaveBeenCalledWith(message, false, false);
    expect(context.service.handleEvent).not.toHaveBeenCalled();
    expect(context.logger.error.mock.calls).toEqual([
      [
        {
          kind: 'bet.settled',
          bodyBytes: 16_385,
          previousDeliveries: 0,
          reason: 'size_limit',
        },
        'Event rejected',
      ],
    ]);
  });
});

describe('database failure handling at the AMQP boundary', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
  const unavailable = () =>
    new DrizzleQueryError(
      'select pg_advisory_xact_lock(hashtextextended($1, 0))',
      ['bet-1'],
      Object.assign(new Error('connect ECONNREFUSED 10.0.0.5:5432'), {
        code: 'ECONNREFUSED',
      }),
    );

  it('requeues without a retry and pauses while PostgreSQL is unavailable', async () => {
    vi.useFakeTimers();
    const context = setup();
    context.service.handleEvent.mockRejectedValueOnce(unavailable());
    const message = delivery();

    await expect(
      context.consumer.handleDelivery(context.channel, message, context.retry),
    ).resolves.toBe('infrastructure_error');
    expect(context.channel.nack).toHaveBeenCalledWith(message, false, true);
    expect(context.channel.reject).not.toHaveBeenCalled();
    expect(context.retry).not.toHaveBeenCalled();
    expect(context.logger.error.mock.calls).toEqual([
      [
        { code: 'ECONNREFUSED' },
        'PostgreSQL unavailable; event consumption paused until it recovers',
      ],
    ]);
    expect(JSON.stringify(context.logger.error.mock.calls)).not.toContain(
      '10.0.0.5',
    );

    // A delivery that reaches storage while paused is returned untouched.
    const next = delivery({ ...bet, betId: 'bet-2' });
    await expect(
      context.consumer.handleDelivery(context.channel, next, context.retry),
    ).resolves.toBe('deferred');
    expect(context.service.handleEvent).toHaveBeenCalledOnce();
    expect(context.channel.nack).toHaveBeenLastCalledWith(next, false, true);

    // The first probe runs after one second and ends the pause.
    await vi.advanceTimersByTimeAsync(999);
    expect(context.service.checkDatabase).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(context.service.checkDatabase).toHaveBeenCalledOnce();
    await expect(
      context.consumer.handleDelivery(context.channel, next, context.retry),
    ).resolves.toBe('settled');
    expect(context.channel.ack).toHaveBeenCalledWith(next);
    await context.consumer.onModuleDestroy();
  });

  it('dead-letters data PostgreSQL refuses and logs its SQLSTATE without SQL', async () => {
    const context = setup();
    context.service.handleEvent.mockRejectedValue(
      new DrizzleQueryError(
        'insert into "player_scores" values ($1, $2)',
        ['alice', '-1'],
        Object.assign(new Error('new row violates check constraint'), {
          code: '23514',
        }),
      ),
    );
    const message = delivery();
    await expect(
      context.consumer.handleDelivery(context.channel, message, context.retry),
    ).resolves.toBe('data_error');
    expect(context.channel.nack).toHaveBeenCalledWith(message, false, false);
    expect(context.channel.reject).not.toHaveBeenCalled();
    expect(context.retry).not.toHaveBeenCalled();
    expect(context.logger.error.mock.calls).toEqual([
      [
        {
          kind: 'bet.settled',
          bodyBytes: message.content.length,
          previousDeliveries: 0,
          code: '23514',
          reason: 'invalid_data',
        },
        'Event rejected',
      ],
    ]);
    expect(JSON.stringify(context.logger.error.mock.calls)).not.toContain(
      'insert into',
    );
  });

  it('backs off on lock_timeout while independent deliveries keep committing', async () => {
    const context = setup();
    context.service.handleEvent.mockRejectedValueOnce(
      new DrizzleQueryError(
        'select pg_advisory_xact_lock(hashtextextended($1, 0))',
        ['bet-1'],
        Object.assign(new Error('canceling statement due to lock timeout'), {
          code: '55P03',
        }),
      ),
    );
    let endBackoff!: () => void;
    context.retry.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          endBackoff = () => resolve(undefined);
        }),
    );
    const blocked = delivery();
    const independent = delivery({ ...bet, betId: 'bet-2', playerId: 'bob' });
    const processing = context.consumer.handleDelivery(
      context.channel,
      blocked,
      context.retry,
    );
    await flush();

    expect(context.retry).toHaveBeenCalledOnce();
    expect(context.channel.reject).not.toHaveBeenCalled();
    await expect(
      context.consumer.handleDelivery(
        context.channel,
        independent,
        context.retry,
      ),
    ).resolves.toBe('settled');
    expect(context.channel.ack).toHaveBeenCalledWith(independent);
    expect(context.channel.nack).not.toHaveBeenCalled();

    endBackoff();
    await expect(processing).resolves.toBe('retryable_error');
    expect(context.channel.reject).toHaveBeenCalledWith(blocked, true);
    expect(context.service.checkDatabase).not.toHaveBeenCalled();
  });

  it('grows the reject delay with the quorum delivery count', async () => {
    const context = setup();
    context.service.handleEvent.mockRejectedValue(
      Object.assign(new Error('could not serialize access'), {
        code: '40001',
      }),
    );
    const message = {
      ...delivery(),
      properties: { headers: { 'x-delivery-count': 3 } },
    } as unknown as ConsumeMessage;
    await expect(
      context.consumer.handleDelivery(context.channel, message, context.retry),
    ).resolves.toBe('retryable_error');
    expect(context.retry).toHaveBeenCalledWith(3);
    expect(context.channel.reject).toHaveBeenCalledWith(message, true);
    expect(context.logger.error.mock.calls).toEqual([
      [
        {
          kind: 'bet.settled',
          bodyBytes: message.content.length,
          previousDeliveries: 3,
          code: '40001',
          reason: 'unknown',
        },
        'Event transaction failed; retrying through broker delivery limit',
      ],
    ]);
  });

  it('cuts the retry backoff short on shutdown and still rejects', async () => {
    const context = setup();
    context.service.handleEvent.mockRejectedValue(new Error('unexpected'));
    const message = {
      ...delivery(),
      properties: { headers: { 'x-delivery-count': 4 } },
    } as unknown as ConsumeMessage;
    const startedAt = performance.now();
    // Default backoff: 16 s for the fifth delivery.
    const outcome = context.consumer.handleDelivery(context.channel, message);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(context.channel.reject).not.toHaveBeenCalled();
    await context.consumer.onModuleDestroy();
    await expect(outcome).resolves.toBe('retryable_error');
    expect(context.channel.reject).toHaveBeenCalledWith(message, true);
    expect(performance.now() - startedAt).toBeLessThan(2000);
  });

  it('does not reject a committed event when the acknowledgement fails', async () => {
    const context = setup();
    context.channel.ack.mockImplementation(() => {
      throw new Error('Channel closed');
    });
    await expect(
      context.consumer.handleDelivery(
        context.channel,
        delivery(),
        context.retry,
      ),
    ).rejects.toThrow('Channel closed');
    expect(context.channel.reject).not.toHaveBeenCalled();
    expect(context.channel.nack).not.toHaveBeenCalled();
    expect(context.retry).not.toHaveBeenCalled();
    expect(context.logger.error).not.toHaveBeenCalled();
  });

  it('stores deliveries in batches, keeps a player in order and caps transactions', async () => {
    const context = setup(
      parseConfig({
        DATABASE_URL: 'postgresql://localhost/db',
        RABBITMQ_URL: 'amqp://localhost',
        EXCHANGE_RATES: 'USD=1',
        EVENT_BATCH_SIZE: '2',
        EVENT_BATCH_CONCURRENCY: '2',
      }),
    );
    const batches: string[][] = [];
    const release: (() => void)[] = [];
    let running = 0;
    let peak = 0;
    context.service.handleBatch.mockImplementation(
      async (inputs: BetEventInput[]) => {
        running += 1;
        peak = Math.max(peak, running);
        batches.push(inputs.map(({ event }) => event.betId));
        await new Promise<void>((resolve) => release.push(resolve));
        running -= 1;
        return inputs.map(() => 'settled');
      },
    );
    // Two lanes: alice and dave share one, bob and carol the other.
    const messages = [
      ['alice-1', 'alice'],
      ['bob-1', 'bob'],
      ['alice-2', 'alice'],
      ['carol-1', 'carol'],
      ['alice-3', 'alice'],
      ['dave-1', 'dave'],
    ].map(([betId, playerId]) => delivery({ ...bet, betId, playerId }));
    const work = messages.map((message) =>
      context.consumer.handleDelivery(context.channel, message, context.retry),
    );

    await flush();
    expect(batches).toEqual([
      ['alice-1', 'alice-2'],
      ['bob-1', 'carol-1'],
    ]);
    release.shift()!();
    await flush();
    // Acknowledged after their batch commits; alice-3 follows in the next batch.
    expect(context.channel.ack.mock.calls).toEqual([
      [messages[0]],
      [messages[2]],
    ]);
    expect(batches[2]).toEqual(['alice-3', 'dave-1']);
    release.splice(0).forEach((resolve) => resolve());

    await expect(Promise.all(work)).resolves.toEqual(
      Array.from({ length: 6 }, () => 'settled'),
    );
    expect(peak).toBe(2);
    expect(context.channel.ack).toHaveBeenCalledTimes(6);
  });
});
