import { z } from 'zod';

export const Environment = {
  DEVELOPMENT: 'development',
  PRODUCTION: 'production',
  TEST: 'test',
} as const;

const ExchangeRateSchema = z
  .string()
  .regex(/^(0|[1-9]\d{0,17})(\.\d{1,6})?$/)
  .refine((value) => /[1-9]/.test(value), 'Exchange rate must be positive');

export const ExchangeRatesSchema = z
  .record(z.string().regex(/^[A-Z]{3}$/), ExchangeRateSchema)
  .refine(
    (rates) => rates.USD !== undefined && /^1(?:\.0{1,6})?$/.test(rates.USD),
    'USD exchange rate must equal 1',
  );

export const ConfigSchema = z.object({
  NODE_ENV: z
    .enum([Environment.PRODUCTION, Environment.DEVELOPMENT, Environment.TEST])
    .default(Environment.DEVELOPMENT),

  PORT: z.coerce.number().int().min(1).max(65535).default(3000),

  DATABASE_URL: z.url().refine((value) => /^postgres(?:ql)?:\/\//.test(value)),
  DATABASE_MAX_POOL: z.coerce.number().int().min(1).default(8),
  DATABASE_HTTP_MAX_POOL: z.coerce.number().int().min(1).default(8),
  DATABASE_HTTP_STATEMENT_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .min(100)
    .max(600_000)
    .default(2_000),
  DATABASE_HTTP_CONNECTION_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .min(100)
    .max(60_000)
    .default(1_000),

  RABBITMQ_URL: z.url().refine((value) => /^amqps?:\/\//.test(value)),
  RABBITMQ_EXCHANGE: z.string().min(1).default('bets'),
  RABBITMQ_QUEUE: z.string().min(1).default('tournament.bets'),
  RABBITMQ_DEAD_LETTER_EXCHANGE: z.string().min(1).default('bets.dlx'),
  RABBITMQ_DEAD_LETTER_QUEUE: z.string().min(1).default('tournament.bets.dlq'),
  RABBITMQ_DEAD_LETTER_ROUTING_KEY: z.string().min(1).default('failed'),
  RABBITMQ_PREFETCH: z.coerce.number().int().min(1).max(65535).default(1000),
  RABBITMQ_MAX_DELIVERY_ATTEMPTS: z.coerce.number().int().min(1).default(5),

  EVENT_BATCH_SIZE: z.coerce.number().int().min(1).max(1000).default(250),
  EVENT_BATCH_CONCURRENCY: z.coerce.number().int().min(1).max(32).default(2),

  EXCHANGE_RATES: z.string().max(10000),

  LOG_LEVEL: z
    .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
    .default('error'),

  LOKI_URL: z.preprocess(
    (value) =>
      typeof value === 'string' && value.trim() === '' ? undefined : value,
    z
      .url()
      .refine(
        (value) => value.startsWith('http://') || value.startsWith('https://'),
        'LOKI_URL must be an absolute HTTP(S) URL',
      )
      .optional(),
  ),
});

export type ConfigSchema = z.infer<typeof ConfigSchema>;
