import { ServiceUnavailableException } from '@nestjs/common';
import { DrizzleQueryError } from 'drizzle-orm';
import { describe, expect, it, vi } from 'vitest';
import type { DatabaseService } from '../src/db';
import { LeaderboardService } from '../src/tournament/services';

function serviceFailingWith(error: unknown) {
  const reader = { execute: vi.fn().mockRejectedValue(error) };
  return new LeaderboardService({ reader } as unknown as DatabaseService);
}

describe('leaderboard read path under load', () => {
  it.each([
    [
      'a saturated HTTP pool',
      new Error('timeout exceeded when trying to connect'),
    ],
    [
      'statement_timeout',
      new DrizzleQueryError(
        'with top_players as (...)',
        ['alice'],
        Object.assign(
          new Error('canceling statement due to statement timeout'),
          {
            code: '57014',
          },
        ),
      ),
    ],
    [
      'an unreachable database',
      Object.assign(new Error('connect ECONNREFUSED'), {
        code: 'ECONNREFUSED',
      }),
    ],
    [
      'lock_timeout',
      new DrizzleQueryError(
        'with top_players as (...)',
        ['alice'],
        Object.assign(new Error('canceling statement due to lock timeout'), {
          code: '55P03',
        }),
      ),
    ],
  ])('sheds load with 503 on %s', async (_name, error) => {
    await expect(
      serviceFailingWith(error).leaderboard('alice'),
    ).rejects.toBeInstanceOf(ServiceUnavailableException);
  });

  it('keeps unexpected failures as server errors', async () => {
    const error = new Error('unexpected');
    await expect(serviceFailingWith(error).leaderboard('alice')).rejects.toBe(
      error,
    );
  });

  it('reads through the HTTP pool, not the event-write pool', async () => {
    const row = { top: [], player: null };
    const reader = { execute: vi.fn().mockResolvedValue({ rows: [row] }) };
    const db = { execute: vi.fn() };
    const service = new LeaderboardService({
      reader,
      db,
    } as unknown as DatabaseService);
    await expect(service.leaderboard('alice')).resolves.toBe(row);
    expect(reader.execute).toHaveBeenCalledOnce();
    expect(db.execute).not.toHaveBeenCalled();
  });
});
