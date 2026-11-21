import type { MessageProperties, Options } from 'amqplib';
import { type EventKind, EventKindSchema } from '../contracts';

export interface DeadLetter {
  /** The original event kind, carried in the routing key or in the type of a redriven copy. */
  routingKey: EventKind;
  /** `rejected` (dead-lettered at once) or `delivery_limit` (retries exhausted). */
  reason: string;
  /** How many times the message died for that reason in that queue. */
  count: number;
}

/** Strip delivery history and routing controls before sending to one queue. */
const DELIVERY_HEADER =
  /^(?:CC|BCC|x-(?:death|delivery-count|acquired-count|opt-delivery-time|(?:first|last)-death-.+))$/;

const headerString = (value: unknown) =>
  Buffer.isBuffer(value) ? value.toString('utf8') : value;

/** Recover the event kind from a dead letter originating in the tournament queue. */
export function readDeadLetter(
  properties: Pick<Partial<MessageProperties>, 'headers' | 'type'>,
  sourceQueue: string,
): DeadLetter | undefined {
  const deaths: unknown = properties.headers?.['x-death'];
  if (!Array.isArray(deaths)) return undefined;
  for (const death of deaths as unknown[]) {
    if (typeof death !== 'object' || death === null) continue;
    const record = death as Record<string, unknown>;
    if (headerString(record.queue) !== sourceQueue) continue;
    const routingKeys: unknown = record['routing-keys'];
    if (!Array.isArray(routingKeys)) continue;
    const key = headerString(routingKeys[0]);
    const direct = headerString(record.exchange) === '';
    if (direct && key !== sourceQueue) continue;
    const kind = EventKindSchema.safeParse(direct ? properties.type : key);
    if (kind.success) {
      return {
        routingKey: kind.data,
        reason: typeof record.reason === 'string' ? record.reason : 'unknown',
        count: typeof record.count === 'number' ? record.count : 0,
      };
    }
  }
  return undefined;
}

/**
 * Publish options that reproduce the original message. `userId` and `expiration` are
 * dropped: the broker rejects a foreign user id and an old TTL could discard the event.
 */
export function republishOptions(
  properties: Partial<MessageProperties>,
): Options.Publish {
  const options: Options.Publish = {
    persistent: true,
    mandatory: true,
    headers: Object.fromEntries(
      Object.entries(properties.headers ?? {}).filter(
        ([name]) => !DELIVERY_HEADER.test(name),
      ),
    ),
  };
  if (typeof properties.contentType === 'string') {
    options.contentType = properties.contentType;
  }
  if (typeof properties.contentEncoding === 'string') {
    options.contentEncoding = properties.contentEncoding;
  }
  if (typeof properties.messageId === 'string') {
    options.messageId = properties.messageId;
  }
  if (typeof properties.correlationId === 'string') {
    options.correlationId = properties.correlationId;
  }
  if (typeof properties.appId === 'string') options.appId = properties.appId;
  if (typeof properties.timestamp === 'number') {
    options.timestamp = properties.timestamp;
  }
  return options;
}
