import { DrizzleQueryError } from 'drizzle-orm';
import { describe, expect, it } from 'vitest';
import {
  classifyDatabaseFailure,
  DatabaseFailure,
  databaseErrorCode,
} from '../src/db';

const withCode = (code: string, message = 'driver failure') =>
  Object.assign(new Error(message), { code });
const wrapped = (cause: unknown) =>
  new DrizzleQueryError('select $1', ['sensitive-parameter'], cause as Error);

describe('database failure classification', () => {
  it.each([
    ['connection refused', withCode('ECONNREFUSED')],
    ['connection reset', withCode('ECONNRESET')],
    ['DNS failure', withCode('ENOTFOUND')],
    ['admin shutdown', withCode('57P01')],
    ['cannot connect now', withCode('57P03')],
    ['statement timeout', withCode('57014')],
    ['too many connections', withCode('53300')],
    ['connection exception', withCode('08006')],
    ['invalid password', withCode('28P01')],
    ['missing table', withCode('42P01')],
    ['missing privilege', withCode('42501')],
    [
      'pool checkout timeout',
      new Error('timeout exceeded when trying to connect'),
    ],
    ['terminated connection', new Error('Connection terminated unexpectedly')],
    [
      'connection timeout',
      new Error('Connection terminated due to connection timeout'),
    ],
    [
      'closed pool',
      new Error('Cannot use a pool after calling end on the pool'),
    ],
  ])('treats %s as an unavailable database', (_name, error) => {
    expect(classifyDatabaseFailure(error)).toBe(DatabaseFailure.Unavailable);
    expect(classifyDatabaseFailure(wrapped(error))).toBe(
      DatabaseFailure.Unavailable,
    );
  });

  it('distinguishes lock_timeout from an unavailable database', () => {
    const timeout = withCode('55P03');
    expect(classifyDatabaseFailure(timeout)).toBe(DatabaseFailure.Contention);
    expect(classifyDatabaseFailure(wrapped(timeout))).toBe(
      DatabaseFailure.Contention,
    );
    expect(
      classifyDatabaseFailure(
        new AggregateError([timeout, withCode('ECONNRESET')]),
      ),
    ).toBe(DatabaseFailure.Unavailable);
  });

  it.each([
    ['check violation', '23514'],
    ['unique violation', '23505'],
    ['numeric overflow', '22003'],
    ['invalid datetime', '22008'],
  ])('treats %s as invalid event data', (_name, code) => {
    expect(classifyDatabaseFailure(wrapped(withCode(code)))).toBe(
      DatabaseFailure.InvalidData,
    );
  });

  it.each([
    ['serialization failure', withCode('40001')],
    ['deadlock', withCode('40P01')],
    ['internal error', withCode('XX000')],
    ['plain error', new Error('sensitive database details')],
    ['non-error value', 'boom'],
    ['nothing', undefined],
  ])('leaves %s to bounded retries', (_name, error) => {
    expect(classifyDatabaseFailure(error)).toBe(DatabaseFailure.Unknown);
  });

  it('finds network errors inside AggregateError from dual-stack connects', () => {
    const error = new AggregateError(
      [withCode('ECONNREFUSED'), withCode('ECONNREFUSED')],
      'connect failed',
    );
    expect(classifyDatabaseFailure(error)).toBe(DatabaseFailure.Unavailable);
  });

  it('prefers unavailability over data errors anywhere in the cause chain', () => {
    const error = Object.assign(withCode('23514'), {
      cause: withCode('57P01'),
    });
    expect(classifyDatabaseFailure(error)).toBe(DatabaseFailure.Unavailable);
  });

  it('stops on cyclic causes', () => {
    const error = withCode('XX000') as Error & { cause?: unknown };
    error.cause = error;
    expect(classifyDatabaseFailure(error)).toBe(DatabaseFailure.Unknown);
  });

  it('reports the first code in the chain without exposing SQL parameters', () => {
    const error = wrapped(withCode('23514'));
    expect(databaseErrorCode(error)).toBe('23514');
    expect(databaseErrorCode(new Error('no code'))).toBeUndefined();
    expect(JSON.stringify({ code: databaseErrorCode(error) })).not.toContain(
      'sensitive-parameter',
    );
  });

  it('logs only recognized system codes and SQLSTATE-shaped values', () => {
    const outer = withCode('sensitive-event-data');
    expect(databaseErrorCode(outer)).toBeUndefined();
    expect(
      databaseErrorCode(Object.assign(outer, { cause: withCode('ETIMEDOUT') })),
    ).toBe('ETIMEDOUT');
  });
});
