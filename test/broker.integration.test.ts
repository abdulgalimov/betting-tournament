import { randomUUID } from 'node:crypto';
import { connect, type ConsumeMessage } from 'amqplib';
import { BetConsumer } from '../src/tournament/messaging';
import { AppConfig } from '../src/config';
import type { BetEvent } from '../src/contracts';
import { describe, expect, it, vi } from 'vitest';
import { PermanentEventError } from '../src/tournament/errors';
import type { BetEventsService } from '../src/tournament/services';
import { batchOf } from './support/batch-service';

const connectionUrl = process.env.TEST_RABBITMQ_URL ?? process.env.RABBITMQ_URL;
const config = new AppConfig();

const validEvent = (): BetEvent => ({
  betId: randomUUID(),
  playerId: 'broker-test-player',
  currency: 'USD',
  stake: '1',
  winAmount: '2',
  settledAt: '2026-11-18T12:00:00.000Z',
});

/** Exercise a real broker without creating or changing the application topology. */
async function deliverUntilDeadLettered(
  body: unknown,
  service: Pick<BetEventsService, 'handleEvent'>,
) {
  const connection = await connect(connectionUrl!, { timeout: 5_000 });
  const channel = await connection.createConfirmChannel();
  const suffix = randomUUID();
  const exchange = `test.broker.${suffix}`;
  const queue = `test.broker.${suffix}`;
  const logger = { error: vi.fn() };
  const consumer = new BetConsumer(
    { ...config, rabbitmq: { ...config.rabbitmq, url: connectionUrl! } },
    {
      ...service,
      handleBatch: batchOf(service.handleEvent),
    } as unknown as BetEventsService,
  );
  Object.defineProperty(consumer, 'logger', { value: logger });
  let deliveries = 0;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let rejectDelivery: (error: unknown) => void = () => undefined;
  const failed = new Promise<never>((_resolve, reject) => {
    rejectDelivery = reject;
  });
  connection.on('error', rejectDelivery);
  channel.on('error', rejectDelivery);

  try {
    await channel.assertExchange(exchange, 'direct', { durable: false });
    const mirror = await channel.assertQueue('', {
      durable: false,
      exclusive: true,
      autoDelete: true,
      arguments: { 'x-queue-type': 'classic' },
    });
    await channel.assertQueue(queue, {
      durable: true,
      arguments: {
        'x-queue-type': 'quorum',
        'x-delivery-limit': config.rabbitmq.maxDeliveryAttempts,
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': mirror.queue,
        'x-dead-letter-strategy': 'at-least-once',
        'x-overflow': 'reject-publish',
      },
    });
    await channel.bindQueue(queue, exchange, 'bet.settled');
    await channel.prefetch(1);

    let resolveDeadLetter: (message: ConsumeMessage) => void = () => undefined;
    const deadLetter = new Promise<ConsumeMessage>((resolve) => {
      resolveDeadLetter = resolve;
    });
    await channel.consume(mirror.queue, (message) => {
      if (!message) return;
      channel.ack(message);
      resolveDeadLetter(message);
    });
    await channel.consume(queue, (message) => {
      if (!message) return;
      deliveries += 1;
      void consumer
        .handleDelivery(channel, message, () => Promise.resolve())
        .catch(rejectDelivery);
    });

    channel.publish(
      exchange,
      'bet.settled',
      Buffer.from(JSON.stringify(body)),
      {
        persistent: true,
        contentType: 'application/json',
      },
    );
    await channel.waitForConfirms();
    const message = await Promise.race([
      deadLetter,
      failed,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(
          () => reject(new Error('Timed out waiting for test dead letter')),
          12_000,
        );
      }),
    ]);
    const deaths = message.properties.headers?.['x-death'] as
      { reason: string; queue: string; count: number }[] | undefined;
    return { message, deaths, deliveries, logger };
  } finally {
    clearTimeout(timeout);
    // Quorum queues survive a connection close, so delete this suite's exact queue.
    await channel.deleteQueue(queue).catch(() => undefined);
    await channel.deleteExchange(exchange).catch(() => undefined);
    await channel.close().catch(() => undefined);
    await connection.close().catch(() => undefined);
  }
}

describe.skipIf(!connectionUrl)(
  'RabbitMQ quorum redelivery and dead lettering',
  () => {
    it('bounds transient retries through the real basic.reject delivery counter', async () => {
      const handleEvent = vi.fn(async () => {
        throw new Error('temporary database outage');
      });
      const event = validEvent();
      const result = await deliverUntilDeadLettered(event, {
        handleEvent,
      });

      expect(result.deaths?.[0]).toMatchObject({
        reason: 'delivery_limit',
        count: 1,
      });
      // Five permitted redeliveries follow the original delivery.
      expect(result.deliveries).toBe(config.rabbitmq.maxDeliveryAttempts + 1);
      expect(handleEvent).toHaveBeenCalledTimes(result.deliveries);
      expect(result.logger.error).toHaveBeenCalledTimes(result.deliveries);
      expect(result.logger.error.mock.calls).toEqual(
        Array.from({ length: result.deliveries }, (_, previousDeliveries) => [
          {
            kind: 'bet.settled',
            bodyBytes: result.message.content.length,
            previousDeliveries,
            reason: 'unknown',
          },
          'Event transaction failed; retrying through broker delivery limit',
        ]),
      );
      expect(JSON.stringify(result.logger.error.mock.calls)).not.toContain(
        'temporary database outage',
      );
    });

    it('dead letters malformed events once without calling the domain service', async () => {
      const handleEvent = vi.fn(async () => 'settled' as const);
      const result = await deliverUntilDeadLettered(
        { betId: 'incomplete' },
        { handleEvent },
      );

      expect(result.deaths?.[0]).toMatchObject({
        reason: 'rejected',
        count: 1,
      });
      expect(result.deliveries).toBe(1);
      expect(handleEvent).not.toHaveBeenCalled();
      expect(result.logger.error.mock.calls).toEqual([
        [
          {
            kind: 'bet.settled',
            bodyBytes: result.message.content.length,
            previousDeliveries: 0,
            reason: 'invalid_contract',
          },
          'Event rejected',
        ],
      ]);
    });

    it('dead letters permanent domain conflicts without retrying', async () => {
      const handleEvent = vi.fn(async () => {
        throw new PermanentEventError(
          'Conflicting settlement for an existing betId',
        );
      });
      const event = validEvent();
      const result = await deliverUntilDeadLettered(event, {
        handleEvent,
      });

      expect(result.deaths?.[0]).toMatchObject({
        reason: 'rejected',
        count: 1,
      });
      expect(result.deliveries).toBe(1);
      expect(handleEvent).toHaveBeenCalledOnce();
      expect(result.logger.error.mock.calls).toEqual([
        [
          {
            kind: 'bet.settled',
            bodyBytes: result.message.content.length,
            previousDeliveries: 0,
            reason: 'domain_error',
          },
          'Event rejected',
        ],
      ]);
    });
  },
);
