import { EventOutcome } from '../services';

export const EventHandleOutcome = {
  ...EventOutcome,
  /** Invalid contract or domain conflict: dead-lettered at once. */
  PermanentError: 'permanent_error',
  /** PostgreSQL rejected the event data (SQLSTATE 22/23): dead-lettered at once. */
  DataError: 'data_error',
  /** PostgreSQL unavailable: requeued without a retry, consumption paused. */
  InfrastructureError: 'infrastructure_error',
  /** Requeued untouched because consumption was paused before it reached storage. */
  Deferred: 'deferred',
  /** Lock contention or another retryable failure: retried up to the broker limit. */
  RetryableError: 'retryable_error',
  UnknownError: 'unknown_error',
} as const;
export type EventHandleOutcome =
  (typeof EventHandleOutcome)[keyof typeof EventHandleOutcome];
