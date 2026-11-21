/** How a failed database call should be handled by its caller. */
export const DatabaseFailure = {
  /**
   * PostgreSQL is unreachable, overloaded, shutting down or misconfigured.
   * No event can succeed right now, so the failure says nothing about the event.
   */
  Unavailable: 'unavailable',
  /** A transaction timed out waiting for a lock. Other work can still succeed. */
  Contention: 'contention',
  /** PostgreSQL rejected the event's data: the same event can never succeed. */
  InvalidData: 'invalid_data',
  /** Anything else: retry with backoff, bounded by the broker delivery limit. */
  Unknown: 'unknown',
} as const;
export type DatabaseFailure =
  (typeof DatabaseFailure)[keyof typeof DatabaseFailure];

/** Socket-level failures reported by Node.js while reaching PostgreSQL. */
const NETWORK_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ECONNABORTED',
  'ETIMEDOUT',
  'EPIPE',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENETDOWN',
]);

/**
 * SQLSTATE classes that describe the database or the deployment rather than the event:
 * 08 connection, 28 authorization, 3D missing database, 42 missing table or privilege
 * (for example, migrations not applied), 53 insufficient resources, 57 operator
 * intervention (including statement_timeout) and 58 system errors.
 */
const UNAVAILABLE_CLASSES = new Set(['08', '28', '3D', '42', '53', '57', '58']);

/** 55P03 lock_not_available: lock_timeout expired while waiting for a lock. */
const LOCK_NOT_AVAILABLE = '55P03';

/** Errors that pg and pg-pool raise without a code. */
const UNAVAILABLE_MESSAGES = [
  'timeout exceeded when trying to connect',
  'Connection terminated',
  'Client has encountered a connection error and is not queryable',
  'Client was closed and is not queryable',
  'Cannot use a pool after calling end on the pool',
];

const SQLSTATE = /^[0-9A-Z]{5}$/;
const MAX_CAUSE_DEPTH = 8;

/**
 * Classify a failure from pg, pg-pool or Drizzle. Drizzle wraps driver errors in
 * DrizzleQueryError, so the whole `cause` chain (and AggregateError members) is inspected.
 */
export function classifyDatabaseFailure(error: unknown): DatabaseFailure {
  let contention = false;
  let invalidData = false;
  for (const candidate of failureChain(error)) {
    const code = codeOf(candidate);
    if (code !== undefined) {
      if (NETWORK_CODES.has(code)) return DatabaseFailure.Unavailable;
      if (code === LOCK_NOT_AVAILABLE) contention = true;
      if (SQLSTATE.test(code)) {
        const sqlClass = code.slice(0, 2);
        if (UNAVAILABLE_CLASSES.has(sqlClass))
          return DatabaseFailure.Unavailable;
        if (sqlClass === '22' || sqlClass === '23') invalidData = true;
      }
    }
    const message = candidate instanceof Error ? candidate.message : undefined;
    if (
      message !== undefined &&
      UNAVAILABLE_MESSAGES.some((known) => message.startsWith(known))
    ) {
      return DatabaseFailure.Unavailable;
    }
  }
  if (contention) return DatabaseFailure.Contention;
  return invalidData ? DatabaseFailure.InvalidData : DatabaseFailure.Unknown;
}

/** The SQLSTATE or system error code of a failure, if any: safe to log. */
export function databaseErrorCode(error: unknown): string | undefined {
  for (const candidate of failureChain(error)) {
    const code = codeOf(candidate);
    if (
      code !== undefined &&
      (SQLSTATE.test(code) || NETWORK_CODES.has(code))
    ) {
      return code;
    }
  }
  return undefined;
}

function* failureChain(error: unknown): Generator<unknown> {
  const pending: { value: unknown; depth: number }[] = [
    { value: error, depth: 0 },
  ];
  const seen = new Set<unknown>();
  while (pending.length > 0) {
    const { value, depth } = pending.shift()!;
    if (value === null || value === undefined || seen.has(value)) continue;
    seen.add(value);
    yield value;
    if (depth >= MAX_CAUSE_DEPTH || typeof value !== 'object') continue;
    if ('cause' in value)
      pending.push({ value: value.cause, depth: depth + 1 });
    if (value instanceof AggregateError) {
      for (const member of value.errors) {
        pending.push({ value: member, depth: depth + 1 });
      }
    }
  }
}

function codeOf(value: unknown): string | undefined {
  if (typeof value !== 'object' || value === null || !('code' in value)) {
    return undefined;
  }
  return typeof value.code === 'string' ? value.code : undefined;
}
