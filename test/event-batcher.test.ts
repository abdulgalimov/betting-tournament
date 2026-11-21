import { describe, expect, it, vi } from 'vitest';
import type { BetEvent } from '../src/contracts';
import { PermanentEventError } from '../src/tournament/errors';
import { EventBatcher } from '../src/tournament/messaging/event-batcher';
import type {
  BatchOutcome,
  BetEventInput,
} from '../src/tournament/services/bet-batch';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
// FNV-1a lanes with two lanes: alice and dave share one, bob and carol the other.
const input = (betId: string, playerId: string): BetEventInput => ({
  kind: 'bet.settled',
  event: {
    betId,
    playerId,
    currency: 'USD',
    stake: '1',
    winAmount: '2',
    settledAt: '2026-11-18T12:00:00Z',
  } satisfies BetEvent,
});
const ids = (inputs: BetEventInput[]) => inputs.map(({ event }) => event.betId);
const pgError = (code: string) =>
  Object.assign(new Error(`SQLSTATE ${code}`), { code });

describe('event batcher', () => {
  it('stores events submitted together in one transaction, in delivery order', async () => {
    const store = vi.fn(async (inputs: BetEventInput[]) =>
      inputs.map((): BatchOutcome => 'settled'),
    );
    const batcher = new EventBatcher(store, { maxBatch: 10, concurrency: 1 });

    const outcomes = await Promise.all(
      ['a', 'b', 'c'].map((betId) => batcher.submit(input(betId, 'alice'))),
    );

    expect(outcomes).toEqual(['settled', 'settled', 'settled']);
    expect(store).toHaveBeenCalledOnce();
    expect(ids(store.mock.calls[0]![0])).toEqual(['a', 'b', 'c']);
  });

  it('keeps a player in one lane and runs at most one batch per lane', async () => {
    const batches: string[][] = [];
    const release: (() => void)[] = [];
    let running = 0;
    let peak = 0;
    const store = vi.fn(async (inputs: BetEventInput[]) => {
      running += 1;
      peak = Math.max(peak, running);
      batches.push(ids(inputs));
      await new Promise<void>((resolve) => release.push(resolve));
      running -= 1;
      return inputs.map((): BatchOutcome => 'settled');
    });
    const batcher = new EventBatcher(store, { maxBatch: 2, concurrency: 2 });

    const work = [
      input('alice-1', 'alice'),
      input('bob-1', 'bob'),
      input('alice-2', 'alice'),
      input('carol-1', 'carol'),
      input('alice-3', 'alice'),
      input('dave-1', 'dave'),
    ].map((event) => batcher.submit(event));
    await flush();
    expect(batches).toEqual([
      ['alice-1', 'alice-2'],
      ['bob-1', 'carol-1'],
    ]);
    expect(batcher.queued).toBe(2);

    release.shift()!();
    await flush();
    // The lane continues with its next events only after its batch commits.
    expect(batches[2]).toEqual(['alice-3', 'dave-1']);
    release.splice(0).forEach((resolve) => resolve());
    await flush();
    release.splice(0).forEach((resolve) => resolve());

    await expect(Promise.all(work)).resolves.toHaveLength(6);
    expect(peak).toBe(2);
    expect(batcher.active).toBe(0);
  });

  it('rejects only the event the domain refuses', async () => {
    const conflict = new PermanentEventError('Conflicting settlement');
    const batcher = new EventBatcher(
      async (inputs) =>
        inputs.map(({ event }) =>
          event.betId === 'b' ? conflict : ('settled' as const),
        ),
      { maxBatch: 10, concurrency: 1 },
    );

    const results = await Promise.allSettled(
      ['a', 'b', 'c'].map((betId) => batcher.submit(input(betId, 'alice'))),
    );

    expect(results).toEqual([
      { status: 'fulfilled', value: 'settled' },
      { status: 'rejected', reason: conflict },
      { status: 'fulfilled', value: 'settled' },
    ]);
  });

  it('stores events one by one when an event fails the whole transaction', async () => {
    const overflow = pgError('22003');
    const store = vi.fn(async (inputs: BetEventInput[]) => {
      if (ids(inputs).includes('b')) throw overflow;
      return inputs.map((): BatchOutcome => 'settled');
    });
    const batcher = new EventBatcher(store, { maxBatch: 10, concurrency: 1 });

    const results = await Promise.allSettled(
      ['a', 'b', 'c'].map((betId) => batcher.submit(input(betId, 'alice'))),
    );

    expect(results).toEqual([
      { status: 'fulfilled', value: 'settled' },
      { status: 'rejected', reason: overflow },
      { status: 'fulfilled', value: 'settled' },
    ]);
    expect(store.mock.calls.map(([inputs]) => ids(inputs))).toEqual([
      ['a', 'b', 'c'],
      ['a'],
      ['b'],
      ['c'],
    ]);
  });

  it('fails every event of the batch at once while PostgreSQL is unavailable', async () => {
    const outage = pgError('57P01');
    const store = vi.fn(async () => {
      throw outage;
    });
    const batcher = new EventBatcher(store, { maxBatch: 10, concurrency: 1 });

    const results = await Promise.allSettled(
      ['a', 'b'].map((betId) => batcher.submit(input(betId, 'alice'))),
    );

    expect(results.map((result) => result.status)).toEqual([
      'rejected',
      'rejected',
    ]);
    expect(store).toHaveBeenCalledOnce();
  });

  it('returns a contended batch for retry and continues with the next batch', async () => {
    const timeout = pgError('55P03');
    const store = vi
      .fn<(inputs: BetEventInput[]) => Promise<BatchOutcome[]>>()
      .mockRejectedValueOnce(timeout)
      .mockResolvedValueOnce(['settled']);
    const batcher = new EventBatcher(store, { maxBatch: 2, concurrency: 1 });

    const results = await Promise.allSettled(
      ['a', 'b', 'c'].map((betId) => batcher.submit(input(betId, 'alice'))),
    );

    expect(results).toEqual([
      { status: 'rejected', reason: timeout },
      { status: 'rejected', reason: timeout },
      { status: 'fulfilled', value: 'settled' },
    ]);
    expect(store.mock.calls.map(([inputs]) => ids(inputs))).toEqual([
      ['a', 'b'],
      ['c'],
    ]);
  });

  it('stops isolating events once PostgreSQL becomes unavailable', async () => {
    const outage = pgError('08006');
    const store = vi
      .fn<(inputs: BetEventInput[]) => Promise<BatchOutcome[]>>()
      .mockRejectedValueOnce(new Error('unexpected'))
      .mockResolvedValueOnce(['settled'])
      .mockRejectedValue(outage);
    const batcher = new EventBatcher(store, { maxBatch: 10, concurrency: 1 });

    const results = await Promise.allSettled(
      ['a', 'b', 'c'].map((betId) => batcher.submit(input(betId, 'alice'))),
    );

    expect(results).toEqual([
      { status: 'fulfilled', value: 'settled' },
      { status: 'rejected', reason: outage },
      { status: 'rejected', reason: outage },
    ]);
    expect(store).toHaveBeenCalledTimes(3);
  });

  it('refuses a batch without touching storage when admission fails', async () => {
    const paused = new Error('Event consumption is paused');
    const store = vi.fn(async () => ['settled' as const]);
    const batcher = new EventBatcher(
      store,
      { maxBatch: 10, concurrency: 1 },
      () => {
        throw paused;
      },
    );

    await expect(batcher.submit(input('a', 'alice'))).rejects.toBe(paused);
    expect(store).not.toHaveBeenCalled();
  });
});
