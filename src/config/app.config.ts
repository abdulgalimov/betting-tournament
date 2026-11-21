import { Injectable } from '@nestjs/common';
import { ExchangeRatesSchema, ConfigSchema } from './config.schema';

export function parseConfig(env: NodeJS.ProcessEnv) {
  const raw = ConfigSchema.safeParse(env);
  if (!raw.success) {
    // Report field names only: URLs can contain credentials.
    throw new Error(
      `Invalid environment fields: ${raw.error.issues.map((issue) => issue.path.join('.')).join(', ')}`,
    );
  }
  // Each event of a batch holds up to two advisory locks until commit; PostgreSQL's
  // default lock table (64 per connection) must fit every concurrent batch.
  if (raw.data.EVENT_BATCH_SIZE * raw.data.EVENT_BATCH_CONCURRENCY > 2000) {
    throw new Error(
      'Invalid environment fields: EVENT_BATCH_SIZE, EVENT_BATCH_CONCURRENCY (their product must not exceed 2000)',
    );
  }
  let decoded: unknown;
  try {
    decoded = raw.data.EXCHANGE_RATES.split(';').reduce(
      (acc, item) => {
        const values = item.split('=');
        if (!values[0] || !values[1]) {
          throw new Error('EXCHANGE_RATES must be a string k1=v1;k2=v2;...');
        }

        acc[values[0]] = values[1];
        return acc;
      },
      {} as Record<string, string>,
    );
  } catch {
    throw new Error(
      'EXCHANGE_RATES must be a semicolon-separated list like USD=1;EUR=1.1',
    );
  }

  const rates = ExchangeRatesSchema.safeParse(decoded);
  if (!rates.success) {
    throw new Error(
      'EXCHANGE_RATES must contain positive decimal strings and USD=1',
    );
  }

  return {
    environment: raw.data.NODE_ENV,
    port: raw.data.PORT,
    database: {
      url: raw.data.DATABASE_URL,
      /** Event-write pool: batch transactions plus the outage probe. */
      maxPool: raw.data.DATABASE_MAX_POOL,
      http: {
        maxPool: raw.data.DATABASE_HTTP_MAX_POOL,
        statementTimeoutMs: raw.data.DATABASE_HTTP_STATEMENT_TIMEOUT_MS,
        connectionTimeoutMs: raw.data.DATABASE_HTTP_CONNECTION_TIMEOUT_MS,
      },
    },
    rabbitmq: {
      url: raw.data.RABBITMQ_URL,
      exchange: raw.data.RABBITMQ_EXCHANGE,
      queue: raw.data.RABBITMQ_QUEUE,
      deadLetterExchange: raw.data.RABBITMQ_DEAD_LETTER_EXCHANGE,
      deadLetterQueue: raw.data.RABBITMQ_DEAD_LETTER_QUEUE,
      deadLetterRoutingKey: raw.data.RABBITMQ_DEAD_LETTER_ROUTING_KEY,
      prefetch: raw.data.RABBITMQ_PREFETCH,
      maxDeliveryAttempts: raw.data.RABBITMQ_MAX_DELIVERY_ATTEMPTS,
    },
    events: {
      /** Most events stored in one transaction. */
      batchSize: raw.data.EVENT_BATCH_SIZE,
      /** Batch transactions at once; never more than the event-write pool. */
      batchConcurrency: Math.min(
        raw.data.EVENT_BATCH_CONCURRENCY,
        raw.data.DATABASE_MAX_POOL,
      ),
    },
    exchangeRates: rates.data,
    logger: {
      level: raw.data.LOG_LEVEL,
      lokiUrl: raw.data.LOKI_URL,
    },
  };
}

const parsedConfig = parseConfig(process.env);

@Injectable()
export class AppConfig {
  public readonly environment = parsedConfig.environment;
  public readonly port = parsedConfig.port;
  public readonly database = parsedConfig.database;
  public readonly rabbitmq = parsedConfig.rabbitmq;
  public readonly events = parsedConfig.events;
  public readonly exchangeRates = parsedConfig.exchangeRates;
  public readonly logger = parsedConfig.logger;
}
