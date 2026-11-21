import { describe, expect, it } from 'vitest';
import { BetEventSchema, MoneySchema, PlayerIdSchema } from '../src/contracts';

const event = {
  betId: 'bet-1',
  playerId: 'player-1',
  currency: 'USD',
  stake: '5.123456',
  winAmount: '15.000001',
  settledAt: '2026-11-18T12:00:00Z',
};

describe('tournament event contract', () => {
  it('preserves decimal strings, including amounts beyond JS safe integer range', () => {
    expect(BetEventSchema.parse(event)).toEqual(event);
    expect(MoneySchema.parse('999999999999999999.999999')).toBe(
      '999999999999999999.999999',
    );
  });

  it.each([
    '-1',
    '1e3',
    '01',
    '.1',
    '1.',
    '1.1234567',
    '1000000000000000000',
    'Infinity',
  ])('rejects ambiguous or unsupported money %s', (value) => {
    expect(MoneySchema.safeParse(value).success).toBe(false);
  });

  it('rejects numeric money and unknown fields', () => {
    expect(BetEventSchema.safeParse({ ...event, stake: 5 }).success).toBe(
      false,
    );
    expect(BetEventSchema.safeParse({ ...event, extra: true }).success).toBe(
      false,
    );
  });

  it.each(['', 'has space', '../path', 'x'.repeat(129)])(
    'rejects invalid identifiers %s',
    (value) => {
      expect(PlayerIdSchema.safeParse(value).success).toBe(false);
    },
  );

  it('requires a timestamp with timezone and an uppercase currency', () => {
    expect(
      BetEventSchema.safeParse({ ...event, settledAt: '2026-11-18T12:00:00' })
        .success,
    ).toBe(false);
    expect(
      BetEventSchema.safeParse({ ...event, currency: 'usd' }).success,
    ).toBe(false);
  });

  it.each([
    '2026-11-18T12:00:00Z',
    '2026-11-18T12:00:00.1Z',
    '2026-11-18T12:00:00.123456Z',
    '2026-11-18T15:00:00.123456+03:00',
    '2026-11-18T09:00:00.123456-03:00',
  ])('preserves supported settlement timestamp precision: %s', (settledAt) => {
    expect(BetEventSchema.parse({ ...event, settledAt }).settledAt).toBe(
      settledAt,
    );
  });

  it.each([
    '2026-11-18T12:00:00.1234567Z',
    '2026-11-18T15:00:00.1234567+03:00',
  ])('rejects timestamps that PostgreSQL would round: %s', (settledAt) => {
    expect(BetEventSchema.safeParse({ ...event, settledAt }).success).toBe(
      false,
    );
  });
});
