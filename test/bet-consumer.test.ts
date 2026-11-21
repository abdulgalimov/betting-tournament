import { EventEmitter } from 'node:events';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { parseConfig, type AppConfig } from '../src/config';
import type { BetEventsService } from '../src/tournament/services';
import { batchOf } from './support/batch-service';

const { connectMock } = vi.hoisted(() => ({ connectMock: vi.fn() }));
vi.mock('amqp-connection-manager', () => ({ connect: connectMock }));
import { BetConsumer } from '../src/tournament/messaging';

describe('consumer connection lifecycle', () => {
  function create(rabbitmq: Partial<AppConfig['rabbitmq']> = {}) {
    const raw = Object.assign(new EventEmitter(), {
      assertExchange: vi.fn().mockResolvedValue(undefined),
      assertQueue: vi.fn().mockResolvedValue(undefined),
      bindQueue: vi.fn().mockResolvedValue(undefined),
      prefetch: vi.fn().mockResolvedValue(undefined),
      consume: vi.fn().mockResolvedValue({ consumerTag: 'test-consumer' }),
      cancel: vi.fn().mockResolvedValue(undefined),
    });
    const wrapper = Object.assign(new EventEmitter(), {
      waitForConnect: vi.fn(),
      close: vi.fn().mockResolvedValue(undefined),
    });
    const connection = Object.assign(new EventEmitter(), {
      isConnected: vi.fn().mockReturnValue(true),
      reconnect: vi.fn(),
      close: vi.fn().mockResolvedValue(undefined),
      createChannel: vi
        .fn()
        .mockImplementation(
          (options: { setup: (channel: typeof raw) => Promise<void> }) => {
            wrapper.waitForConnect.mockImplementation(() => options.setup(raw));
            return wrapper;
          },
        ),
    });
    connectMock.mockReturnValue(connection);
    const config = parseConfig({
      DATABASE_URL: 'postgresql://localhost/db',
      RABBITMQ_URL: 'amqp://localhost',
      EXCHANGE_RATES: 'USD=1',
      LOG_LEVEL: 'silent',
    });
    Object.assign(config.rabbitmq, rabbitmq);
    const service = {
      handleEvent: vi.fn(),
      handleBatch: vi.fn(),
      checkDatabase: vi.fn().mockResolvedValue(undefined),
    };
    service.handleBatch.mockImplementation(batchOf(service.handleEvent));
    const consumer = new BetConsumer(
      config,
      service as unknown as BetEventsService,
    );
    const logger = { error: vi.fn(), warn: vi.fn() };
    Object.defineProperty(consumer, 'logger', { value: logger });
    return { consumer, raw, connection, wrapper, service, logger };
  }

  const ack = () =>
    ({
      ack: vi.fn(),
      nack: vi.fn(),
      reject: vi.fn(),
    }) as unknown as ConfirmChannel;
  const message = () =>
    ({
      content: Buffer.from(
        JSON.stringify({
          betId: 'bet-1',
          playerId: 'alice',
          currency: 'USD',
          stake: '1',
          winAmount: '2',
          settledAt: '2026-11-18T12:00:00Z',
        }),
      ),
      fields: { routingKey: 'bet.settled' },
      properties: {},
    }) as unknown as ConsumeMessage;
  const adminShutdown = () =>
    Object.assign(new Error('terminating connection'), { code: '57P01' });

  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('uses injected RabbitMQ settings for topology, prefetch, and consumption', async () => {
    const rabbitmq: AppConfig['rabbitmq'] = {
      url: 'amqp://custom-host',
      exchange: 'custom.bets',
      queue: 'custom.tournament.bets',
      deadLetterExchange: 'custom.bets.dlx',
      deadLetterQueue: 'custom.tournament.bets.dlq',
      deadLetterRoutingKey: 'custom.failed',
      prefetch: 7,
      maxDeliveryAttempts: 2,
    };
    const { consumer, raw } = create(rabbitmq);
    await consumer.onModuleInit();
    expect(connectMock).toHaveBeenCalledWith(
      [rabbitmq.url],
      expect.any(Object),
    );
    expect(raw.assertExchange).toHaveBeenCalledWith(
      rabbitmq.exchange,
      'topic',
      {
        durable: true,
      },
    );
    expect(raw.assertExchange).toHaveBeenCalledWith(
      rabbitmq.deadLetterExchange,
      'direct',
      { durable: true },
    );
    expect(raw.assertQueue).toHaveBeenCalledWith(rabbitmq.deadLetterQueue, {
      durable: true,
      arguments: { 'x-queue-type': 'quorum' },
    });
    expect(raw.assertQueue).toHaveBeenCalledWith(rabbitmq.queue, {
      durable: true,
      arguments: {
        'x-queue-type': 'quorum',
        'x-delivery-limit': rabbitmq.maxDeliveryAttempts,
        'x-dead-letter-exchange': rabbitmq.deadLetterExchange,
        'x-dead-letter-routing-key': rabbitmq.deadLetterRoutingKey,
        'x-dead-letter-strategy': 'at-least-once',
        'x-overflow': 'reject-publish',
      },
    });
    expect(raw.bindQueue).toHaveBeenCalledWith(
      rabbitmq.deadLetterQueue,
      rabbitmq.deadLetterExchange,
      rabbitmq.deadLetterRoutingKey,
    );
    for (const routingKey of ['bet.settled', 'bet.rollback']) {
      expect(raw.bindQueue).toHaveBeenCalledWith(
        rabbitmq.queue,
        rabbitmq.exchange,
        routingKey,
      );
    }
    expect(raw.prefetch).toHaveBeenCalledWith(rabbitmq.prefetch);
    expect(raw.consume).toHaveBeenCalledWith(
      rabbitmq.queue,
      expect.any(Function),
      { noAck: false },
    );
    await consumer.onModuleDestroy();
  });

  it('defers isolated channel recovery until connection close handlers have run', async () => {
    const { consumer, raw, connection } = create();
    await consumer.onModuleInit();
    expect(consumer.isReady()).toBe(true);
    raw.emit('close');
    expect(consumer.isReady()).toBe(false);
    expect(connection.reconnect).not.toHaveBeenCalled();
    await vi.runOnlyPendingTimersAsync();
    expect(connection.reconnect).toHaveBeenCalledOnce();
    await consumer.onModuleDestroy();
  });

  it('lets the manager recover a full connection loss without forcing another close', async () => {
    const { consumer, raw, connection } = create();
    await consumer.onModuleInit();
    // This is the actual amqplib event order during broker shutdown.
    raw.emit('close');
    connection.isConnected.mockReturnValue(false);
    connection.emit('disconnect');
    await vi.runOnlyPendingTimersAsync();
    expect(connection.reconnect).not.toHaveBeenCalled();
    expect(consumer.isReady()).toBe(false);
    await consumer.onModuleDestroy();
  });

  it('cancels scheduled recovery during application shutdown', async () => {
    const { consumer, raw, connection } = create();
    await consumer.onModuleInit();
    raw.emit('close');
    await consumer.onModuleDestroy();
    await vi.runOnlyPendingTimersAsync();
    expect(connection.reconnect).not.toHaveBeenCalled();
  });

  it('cancels consumption while PostgreSQL is unavailable and resumes after a successful probe', async () => {
    const { consumer, raw, service, logger } = create();
    await consumer.onModuleInit();
    expect(raw.consume).toHaveBeenCalledOnce();
    service.handleEvent.mockRejectedValueOnce(adminShutdown());
    service.checkDatabase
      .mockRejectedValueOnce(new Error('still down'))
      .mockResolvedValue(undefined);
    const channel = ack();

    await expect(
      consumer.handleDelivery(channel, message(), async () => undefined),
    ).resolves.toBe('infrastructure_error');
    expect(channel.nack).toHaveBeenCalledWith(expect.anything(), false, true);
    expect(raw.cancel).toHaveBeenCalledWith('test-consumer');
    expect(consumer.isReady()).toBe(false);
    expect(logger.error).toHaveBeenCalledWith(
      { code: '57P01' },
      'PostgreSQL unavailable; event consumption paused until it recovers',
    );

    // The first probe fails after 1 s; the next one waits 2 s and succeeds.
    await vi.advanceTimersByTimeAsync(1000);
    expect(service.checkDatabase).toHaveBeenCalledTimes(1);
    expect(raw.consume).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1999);
    expect(service.checkDatabase).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(service.checkDatabase).toHaveBeenCalledTimes(2);
    expect(raw.consume).toHaveBeenCalledTimes(2);
    expect(consumer.isReady()).toBe(true);
    await consumer.onModuleDestroy();
  });

  it('restores topology without consuming while paused and consumes on resume', async () => {
    const { consumer, raw, connection, wrapper, service } = create();
    await consumer.onModuleInit();
    service.handleEvent.mockRejectedValueOnce(adminShutdown());
    service.checkDatabase.mockRejectedValueOnce(new Error('still down'));
    await consumer.handleDelivery(ack(), message(), async () => undefined);

    // The channel is lost and recreated during the outage.
    raw.emit('close');
    await vi.advanceTimersByTimeAsync(0);
    expect(connection.reconnect).toHaveBeenCalledOnce();
    await wrapper.waitForConnect();
    expect(raw.assertQueue).toHaveBeenCalledTimes(4);
    expect(raw.consume).toHaveBeenCalledOnce();
    expect(consumer.isReady()).toBe(false);

    await vi.advanceTimersByTimeAsync(1000 + 2000);
    expect(service.checkDatabase).toHaveBeenCalledTimes(2);
    expect(raw.consume).toHaveBeenCalledTimes(2);
    expect(consumer.isReady()).toBe(true);
    await consumer.onModuleDestroy();
  });

  it('keeps backing off when PostgreSQL fails again right after a resume', async () => {
    const { consumer, raw, service } = create();
    await consumer.onModuleInit();
    service.handleEvent
      .mockRejectedValueOnce(adminShutdown())
      .mockRejectedValueOnce(adminShutdown());
    await consumer.handleDelivery(ack(), message(), async () => undefined);
    await vi.advanceTimersByTimeAsync(1000);
    expect(raw.consume).toHaveBeenCalledTimes(2);

    // The probe passed but storage still fails: the next probe waits longer.
    await consumer.handleDelivery(ack(), message(), async () => undefined);
    await vi.advanceTimersByTimeAsync(1999);
    expect(service.checkDatabase).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(service.checkDatabase).toHaveBeenCalledTimes(2);
    await consumer.onModuleDestroy();
  });

  it('returns prefetched deliveries untouched while paused', async () => {
    const { consumer, raw, service } = create();
    await consumer.onModuleInit();
    const onMessage = raw.consume.mock.calls[0]![1] as (
      message: ConsumeMessage | null,
    ) => void;
    service.handleEvent.mockRejectedValueOnce(adminShutdown());
    service.checkDatabase.mockRejectedValue(new Error('still down'));
    await consumer.handleDelivery(ack(), message(), async () => undefined);

    const late = message();
    const nack = vi.fn();
    Object.assign(raw, { nack });
    onMessage(late);
    expect(nack).toHaveBeenCalledWith(late, false, true);
    expect(service.handleEvent).toHaveBeenCalledOnce();
    await consumer.onModuleDestroy();
  });

  it('stops probing PostgreSQL during application shutdown', async () => {
    const { consumer, service } = create();
    await consumer.onModuleInit();
    service.handleEvent.mockRejectedValueOnce(adminShutdown());
    await consumer.handleDelivery(ack(), message(), async () => undefined);
    await consumer.onModuleDestroy();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(service.checkDatabase).not.toHaveBeenCalled();
  });
});
