import type { ConsumeMessage } from 'amqplib';

const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 30_000;

/**
 * Delay before `basic.reject(requeue)` for a failure of unknown cause. It doubles with
 * every earlier delivery, so the broker delivery limit spans about a minute instead of
 * a few seconds: 1, 2, 4, 8, 16 and 30 seconds for the default limit of five redeliveries.
 */
export function retryBackoffMs(previousDeliveries: number): number {
  return Math.min(BASE_DELAY_MS * 2 ** previousDeliveries, MAX_DELAY_MS);
}

/** Delay before the next PostgreSQL probe while event consumption is paused. */
export function resumeBackoffMs(failedProbes: number): number {
  return Math.min(BASE_DELAY_MS * 2 ** failedProbes, MAX_DELAY_MS);
}

/** Earlier deliveries of this message as counted by the quorum queue (0 when absent). */
export function previousDeliveries(message: ConsumeMessage): number {
  const value: unknown = message.properties.headers?.['x-delivery-count'];
  if (typeof value !== 'number' && typeof value !== 'string') return 0;
  const count = typeof value === 'number' ? value : Number(value);
  return Number.isSafeInteger(count) && count > 0 ? count : 0;
}
