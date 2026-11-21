import type { XDeath } from 'amqplib';
import { describe, expect, it } from 'vitest';
import { readDeadLetter, republishOptions } from '../src/cli/dead-letter';
import { parseRedriveArguments } from '../src/cli/redrive-arguments';

const death = (overrides: Record<string, unknown> = {}) => ({
  count: 1,
  reason: 'delivery_limit',
  queue: 'tournament.bets',
  exchange: 'bets',
  'routing-keys': ['bet.settled'],
  time: { '!': 'timestamp', value: 1795180000 },
  ...overrides,
});

const read = (headers?: Record<string, unknown>, type?: string) =>
  readDeadLetter({ headers, type }, 'tournament.bets');

describe('dead-letter redrive', () => {
  it('recovers the original event kind replaced by the dead-letter routing key', () => {
    expect(
      read({
        'x-death': [death({ 'routing-keys': ['bet.rollback'], count: 2 })],
      }),
    ).toEqual({
      routingKey: 'bet.rollback',
      reason: 'delivery_limit',
      count: 2,
    });
    expect(
      read({
        'x-death': [death({ 'routing-keys': [Buffer.from('bet.settled')] })],
      })?.routingKey,
    ).toBe('bet.settled');
  });

  it('skips foreign routing keys and malformed headers', () => {
    expect(read()).toBeUndefined();
    expect(read({})).toBeUndefined();
    expect(read({ 'x-death': 'broken' })).toBeUndefined();
    expect(
      read({
        'x-death': [death({ 'routing-keys': ['failed', 'bet.settled'] })],
      }),
    ).toBeUndefined();
    expect(
      read({
        'x-death': [
          null,
          death({ 'routing-keys': ['failed'] }),
          death({ queue: 'other.bets', 'routing-keys': ['bet.rollback'] }),
          death({ reason: 'rejected' }),
        ],
      }),
    ).toEqual({ routingKey: 'bet.settled', reason: 'rejected', count: 1 });
  });

  it('recovers a validated event kind after a direct redrive is dead-lettered again', () => {
    const headers = {
      'x-death': [death({ exchange: '', 'routing-keys': ['tournament.bets'] })],
    };
    expect(read(headers, 'bet.rollback')?.routingKey).toBe('bet.rollback');
    expect(read(headers, 'unrecognized')).toBeUndefined();
    expect(read(headers)).toBeUndefined();
    expect(
      read(
        { 'x-death': [death({ 'routing-keys': ['tournament.bets'] })] },
        'bet.rollback',
      ),
    ).toBeUndefined();
  });

  it('republishes persistently without delivery history or routing controls', () => {
    expect(
      republishOptions({
        contentType: 'application/json',
        messageId: 'message-1',
        timestamp: 1795180000,
        userId: 'producer',
        expiration: '60000',
        type: 'untrusted-original-type',
        headers: {
          // amqplib's XDeath type predates the delivery_limit reason.
          'x-death': [death() as unknown as XDeath],
          'x-first-death-queue': 'tournament.bets',
          'x-first-death-reason': 'delivery_limit',
          'x-last-death-exchange': 'bets',
          'x-delivery-count': 5,
          'x-acquired-count': 7,
          'x-opt-delivery-time': 1795180000,
          CC: ['mirror'],
          BCC: ['another-mirror'],
          traceparent: '00-abc-def-01',
        },
      }),
    ).toEqual({
      persistent: true,
      mandatory: true,
      contentType: 'application/json',
      messageId: 'message-1',
      timestamp: 1795180000,
      headers: { traceparent: '00-abc-def-01' },
    });
  });

  it('defaults to a dry run and validates its arguments', () => {
    expect(parseRedriveArguments([])).toEqual({
      apply: false,
      limit: undefined,
      reason: undefined,
      help: false,
    });
    expect(
      parseRedriveArguments([
        '--apply',
        '--limit',
        '25',
        '--reason',
        'rejected',
      ]),
    ).toEqual({ apply: true, limit: 25, reason: 'rejected', help: false });
    expect(() => parseRedriveArguments(['--limit', '0'])).toThrow('--limit');
    expect(() => parseRedriveArguments(['--limit'])).toThrow('--limit');
    expect(() => parseRedriveArguments(['--reason', 'expired'])).toThrow(
      '--reason',
    );
    expect(() => parseRedriveArguments(['--force'])).toThrow(
      'Unknown argument: --force',
    );
  });
});
