import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { connect, type ConfirmChannel } from 'amqplib';
import { describe, expect, it, vi } from 'vitest';
import { EventKind } from '../src/contracts';

const connectionUrl = process.env.TEST_RABBITMQ_URL!;
const exec = promisify(execFile);
const cli = fileURLToPath(
  new URL('../src/cli/redrive-dlq.ts', import.meta.url),
);

async function withDeadLetter(
  verify: (fixture: {
    channel: ConfirmChannel;
    queue: string;
    deadLetterQueue: string;
    mirror: string;
    body: Buffer;
    runRedrive: () => Promise<{ stdout: string }>;
  }) => Promise<void>,
): Promise<void> {
  const prefix = `test.redrive.${randomUUID()}`;
  const exchange = `${prefix}.events`;
  const deadLetterExchange = `${prefix}.dead-letters`;
  const queue = `${prefix}.bets`;
  const deadLetterQueue = `${prefix}.dlq`;
  const body = Buffer.from(
    JSON.stringify({
      betId: randomUUID(),
      playerId: 'redrive-test-player',
      currency: 'USD',
      stake: '1',
      winAmount: '2',
      settledAt: '2026-11-18T12:00:00.000Z',
    }),
  );
  const connection = await connect(connectionUrl, { timeout: 5000 });
  try {
    const channel = await connection.createConfirmChannel();
    try {
      await channel.assertExchange(exchange, 'topic', { durable: false });
      await channel.assertExchange(deadLetterExchange, 'direct', {
        durable: false,
      });
      await channel.assertQueue(deadLetterQueue, {
        durable: true,
        arguments: { 'x-queue-type': 'quorum' },
      });
      await channel.assertQueue(queue, {
        durable: true,
        arguments: {
          'x-queue-type': 'quorum',
          'x-dead-letter-exchange': deadLetterExchange,
          'x-dead-letter-strategy': 'at-least-once',
          'x-overflow': 'reject-publish',
        },
      });
      const { queue: mirror } = await channel.assertQueue('', {
        exclusive: true,
        durable: false,
        autoDelete: true,
        arguments: { 'x-queue-type': 'classic' },
      });
      await channel.bindQueue(queue, exchange, EventKind.BetRollback);
      await channel.bindQueue(mirror, exchange, EventKind.BetRollback);
      await channel.bindQueue(
        deadLetterQueue,
        deadLetterExchange,
        EventKind.BetRollback,
      );
      await channel.bindQueue(deadLetterQueue, deadLetterExchange, queue);
      channel.publish(exchange, EventKind.BetRollback, body, {
        persistent: true,
        contentType: 'application/json',
        type: 'legacy-publisher-type',
        // This header persists in the DLQ and would add a destination on the default exchange.
        headers: { CC: [mirror] },
      });
      await channel.waitForConfirms();

      const original = await receive(channel, queue);
      channel.reject(original, false);
      const mirrorCopy = await receive(channel, mirror);
      expect(mirrorCopy.content).toEqual(body);
      channel.ack(mirrorCopy);
      await waitForDeadLetter(channel, deadLetterQueue);

      const runRedrive = () =>
        exec(
          process.execPath,
          ['--import', 'tsx', cli, '--apply', '--reason', 'rejected'],
          {
            env: {
              ...process.env,
              NODE_OPTIONS: '',
              RABBITMQ_URL: connectionUrl,
              RABBITMQ_EXCHANGE: exchange,
              RABBITMQ_QUEUE: queue,
              RABBITMQ_DEAD_LETTER_QUEUE: deadLetterQueue,
            },
            timeout: 5000,
          },
        );
      await verify({
        channel,
        queue,
        deadLetterQueue,
        mirror,
        body,
        runRedrive,
      });
    } finally {
      await channel.deleteQueue(queue).catch(() => undefined);
      await channel.deleteQueue(deadLetterQueue).catch(() => undefined);
      await channel.deleteExchange(exchange).catch(() => undefined);
      await channel.deleteExchange(deadLetterExchange).catch(() => undefined);
      await channel.close().catch(() => undefined);
    }
  } finally {
    await connection.close().catch(() => undefined);
  }
}

function receive(channel: ConfirmChannel, queue: string) {
  return vi.waitFor(
    async () => {
      const message = await channel.get(queue, { noAck: false });
      if (!message)
        throw new Error(`Timed out waiting for a test delivery on ${queue}`);
      return message;
    },
    { timeout: 4000 },
  );
}

function waitForDeadLetter(channel: ConfirmChannel, queue: string) {
  return vi.waitFor(
    async () => expect((await channel.checkQueue(queue)).messageCount).toBe(1),
    { timeout: 4000 },
  );
}

describe('DLQ redrive on RabbitMQ', () => {
  it('delivers exclusively to the intended queue and preserves the event kind on a second redrive', async () => {
    await withDeadLetter(
      async ({ channel, queue, deadLetterQueue, mirror, body, runRedrive }) => {
        for (let attempt = 0; attempt < 2; attempt += 1) {
          await runRedrive();
          const delivery = await receive(channel, queue);
          expect(delivery.fields).toMatchObject({
            exchange: '',
            routingKey: queue,
          });
          expect(delivery.properties).toMatchObject({
            type: EventKind.BetRollback,
            contentType: 'application/json',
            deliveryMode: 2,
          });
          expect(delivery.properties.headers).not.toHaveProperty('x-death');
          expect(delivery.properties.headers).not.toHaveProperty('CC');
          expect(delivery.content).toEqual(body);
          expect(await channel.get(mirror, { noAck: true })).toBe(false);
          expect(await channel.get(deadLetterQueue, { noAck: true })).toBe(
            false,
          );

          if (attempt === 0) {
            channel.reject(delivery, false);
            await waitForDeadLetter(channel, deadLetterQueue);
          } else {
            channel.ack(delivery);
          }
        }
      },
    );
  });

  it('leaves the original in the DLQ if only an unrelated route is available', async () => {
    await withDeadLetter(
      async ({ channel, queue, deadLetterQueue, mirror, body, runRedrive }) => {
        await channel.deleteQueue(queue);
        const { stdout } = await runRedrive();
        expect(stdout).toContain(`Moved 0 message(s) to queue ${queue}`);
        expect(await channel.get(mirror, { noAck: true })).toBe(false);
        const original = await receive(channel, deadLetterQueue);
        expect(original.content).toEqual(body);
        expect(original.properties.headers?.['x-death']).toBeDefined();
        expect(original.properties.headers?.CC).toEqual([mirror]);
        channel.ack(original);
      },
    );
  });
});
