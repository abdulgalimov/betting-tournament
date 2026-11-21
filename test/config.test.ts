import { describe, expect, it } from 'vitest';
import { parseConfig } from '../src/config';

const env = {
  DATABASE_URL: 'postgresql://user:secret@localhost/db',
  RABBITMQ_URL: 'amqp://user:secret@localhost',
  EXCHANGE_RATES: 'USD=1;EUR=1.1',
};

describe('tournament startup configuration', () => {
  it('preserves decimal exchange rates as strings', () => {
    expect(parseConfig(env).exchangeRates).toEqual({ USD: '1', EUR: '1.1' });
  });
  it('uses the default database pool size and accepts an integer from the environment', () => {
    expect(parseConfig(env).database).toEqual({
      url: env.DATABASE_URL,
      maxPool: 8,
      http: { maxPool: 8, statementTimeoutMs: 2000, connectionTimeoutMs: 1000 },
    });
    expect(
      parseConfig({ ...env, DATABASE_MAX_POOL: '32' }).database.maxPool,
    ).toBe(32);
  });
  it.each(['0', '-1', '1.5', 'broken'])(
    'rejects an invalid database pool size %s',
    (maxPool) => {
      expect(() => parseConfig({ ...env, DATABASE_MAX_POOL: maxPool })).toThrow(
        'DATABASE_MAX_POOL',
      );
    },
  );
  it('batches events with defaults that fit the event pool', () => {
    expect(parseConfig(env).events).toEqual({
      batchSize: 250,
      batchConcurrency: 2,
    });
    expect(
      parseConfig({
        ...env,
        EVENT_BATCH_SIZE: '100',
        EVENT_BATCH_CONCURRENCY: '4',
      }).events,
    ).toEqual({ batchSize: 100, batchConcurrency: 4 });
    // Never more batch transactions than event-pool connections.
    expect(
      parseConfig({
        ...env,
        DATABASE_MAX_POOL: '1',
        EVENT_BATCH_CONCURRENCY: '4',
      }).events.batchConcurrency,
    ).toBe(1);
  });
  it.each([
    ['EVENT_BATCH_SIZE', '0'],
    ['EVENT_BATCH_SIZE', '1001'],
    ['EVENT_BATCH_SIZE', '1.5'],
    ['EVENT_BATCH_CONCURRENCY', '0'],
    ['EVENT_BATCH_CONCURRENCY', '33'],
    ['EVENT_BATCH_CONCURRENCY', 'broken'],
  ])('rejects an invalid %s value %s', (field, value) => {
    expect(() => parseConfig({ ...env, [field]: value })).toThrow(field);
  });
  it('rejects batches whose advisory locks may not fit the lock table', () => {
    expect(() =>
      parseConfig({
        ...env,
        EVENT_BATCH_SIZE: '1000',
        EVENT_BATCH_CONCURRENCY: '3',
      }),
    ).toThrow('EVENT_BATCH_SIZE, EVENT_BATCH_CONCURRENCY');
    expect(
      parseConfig({
        ...env,
        EVENT_BATCH_SIZE: '1000',
        EVENT_BATCH_CONCURRENCY: '2',
      }).events.batchSize,
    ).toBe(1000);
  });
  it('reads the separate HTTP pool and its timeouts from the environment', () => {
    expect(
      parseConfig({
        ...env,
        DATABASE_HTTP_MAX_POOL: '4',
        DATABASE_HTTP_STATEMENT_TIMEOUT_MS: '1500',
        DATABASE_HTTP_CONNECTION_TIMEOUT_MS: '250',
      }).database.http,
    ).toEqual({
      maxPool: 4,
      statementTimeoutMs: 1500,
      connectionTimeoutMs: 250,
    });
  });
  it.each([
    ['DATABASE_HTTP_MAX_POOL', '0'],
    ['DATABASE_HTTP_MAX_POOL', 'broken'],
    ['DATABASE_HTTP_STATEMENT_TIMEOUT_MS', '99'],
    ['DATABASE_HTTP_STATEMENT_TIMEOUT_MS', '1.5'],
    ['DATABASE_HTTP_CONNECTION_TIMEOUT_MS', '0'],
    ['DATABASE_HTTP_CONNECTION_TIMEOUT_MS', '60001'],
  ])('rejects an invalid %s value %s', (field, value) => {
    expect(() => parseConfig({ ...env, [field]: value })).toThrow(field);
  });
  it('uses RabbitMQ defaults when optional environment settings are absent', () => {
    expect(parseConfig(env).rabbitmq).toEqual({
      url: env.RABBITMQ_URL,
      exchange: 'bets',
      queue: 'tournament.bets',
      deadLetterExchange: 'bets.dlx',
      deadLetterQueue: 'tournament.bets.dlq',
      deadLetterRoutingKey: 'failed',
      prefetch: 1000,
      maxDeliveryAttempts: 5,
    });
  });
  it('reads RabbitMQ resource names and numeric settings from the environment', () => {
    expect(
      parseConfig({
        ...env,
        RABBITMQ_EXCHANGE: 'custom.bets',
        RABBITMQ_QUEUE: 'custom.tournament.bets',
        RABBITMQ_DEAD_LETTER_EXCHANGE: 'custom.bets.dlx',
        RABBITMQ_DEAD_LETTER_QUEUE: 'custom.tournament.bets.dlq',
        RABBITMQ_DEAD_LETTER_ROUTING_KEY: 'custom.failed',
        RABBITMQ_PREFETCH: '32',
        RABBITMQ_MAX_DELIVERY_ATTEMPTS: '3',
      }).rabbitmq,
    ).toEqual({
      url: env.RABBITMQ_URL,
      exchange: 'custom.bets',
      queue: 'custom.tournament.bets',
      deadLetterExchange: 'custom.bets.dlx',
      deadLetterQueue: 'custom.tournament.bets.dlq',
      deadLetterRoutingKey: 'custom.failed',
      prefetch: 32,
      maxDeliveryAttempts: 3,
    });
  });
  it.each([
    'RABBITMQ_EXCHANGE',
    'RABBITMQ_QUEUE',
    'RABBITMQ_DEAD_LETTER_EXCHANGE',
    'RABBITMQ_DEAD_LETTER_QUEUE',
    'RABBITMQ_DEAD_LETTER_ROUTING_KEY',
  ])('rejects an empty %s instead of falling back to its default', (field) => {
    expect(() => parseConfig({ ...env, [field]: '' })).toThrow(field);
  });
  it.each(
    ['RABBITMQ_PREFETCH', 'RABBITMQ_MAX_DELIVERY_ATTEMPTS'].flatMap((field) =>
      ['', '0', '-1', '1.5', 'broken', 'Infinity'].map((value) => [
        field,
        value,
      ]),
    ),
  )('rejects an invalid %s value %s', (field, value) => {
    expect(() => parseConfig({ ...env, [field]: value })).toThrow(field);
  });
  it('rejects prefetch above the AMQP limit and accepts both valid boundaries', () => {
    expect(() => parseConfig({ ...env, RABBITMQ_PREFETCH: '65536' })).toThrow(
      'RABBITMQ_PREFETCH',
    );
    expect(
      parseConfig({ ...env, RABBITMQ_PREFETCH: '1' }).rabbitmq.prefetch,
    ).toBe(1);
    expect(
      parseConfig({ ...env, RABBITMQ_PREFETCH: '65535' }).rabbitmq.prefetch,
    ).toBe(65535);
  });
  it.each([
    'EUR=1.1',
    'USD=2',
    'USD=1;EUR=0',
    'USD=1;EUR=not-a-number',
    'broken',
  ])('rejects invalid rates %s', (rates) => {
    expect(() => parseConfig({ ...env, EXCHANGE_RATES: rates })).toThrow(
      'EXCHANGE_RATES',
    );
  });
  it('reports field names without leaking URL credentials', () => {
    expect(() =>
      parseConfig({ ...env, DATABASE_URL: 'https://user:secret@example.com' }),
    ).toThrow('DATABASE_URL');
    try {
      parseConfig({ ...env, DATABASE_URL: 'https://user:secret@example.com' });
    } catch (error) {
      expect(String(error)).not.toContain('secret');
    }
  });
});
