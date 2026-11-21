import { classifyDatabaseFailure, DatabaseFailure } from '../../db';
import { PermanentEventError } from '../errors';
import type { EventOutcome } from '../services';
import type { BatchOutcome, BetEventInput } from '../services/bet-batch';

interface Submission {
  input: BetEventInput;
  resolve: (outcome: EventOutcome) => void;
  reject: (error: unknown) => void;
}

export interface EventBatcherOptions {
  /** Most events stored in one transaction. */
  maxBatch: number;
  /** Batch transactions at once; each holds one pooled connection. */
  concurrency: number;
}

interface Lane {
  waiting: Submission[];
  busy: boolean;
  scheduled: boolean;
}

/**
 * Stores events in shared transactions. Each player belongs to one lane, and a lane
 * stores one batch at a time in delivery order, so a player's events keep their order
 * and lanes never wait for each other's player locks. A lane takes every waiting event
 * up to maxBatch: an idle service stores an event at once, a busy one stores bigger
 * batches instead of opening more transactions.
 */
export class EventBatcher {
  private readonly lanes: Lane[];

  constructor(
    private readonly store: (
      inputs: BetEventInput[],
    ) => Promise<BatchOutcome[]>,
    private readonly options: EventBatcherOptions,
    /** Throws to refuse a batch before it starts, e.g. while consumption is paused. */
    private readonly admit: () => void = () => undefined,
  ) {
    this.lanes = Array.from({ length: options.concurrency }, () => ({
      waiting: [],
      busy: false,
      scheduled: false,
    }));
  }

  /** Events waiting for a batch. */
  get queued(): number {
    return this.lanes.reduce((sum, lane) => sum + lane.waiting.length, 0);
  }

  /** Batches being stored. */
  get active(): number {
    return this.lanes.filter((lane) => lane.busy).length;
  }

  /** Resolves after the event's transaction commits; rejects with its failure. */
  submit(input: BetEventInput): Promise<EventOutcome> {
    const lane = this.lanes[laneOf(input.event.playerId, this.lanes.length)]!;
    return new Promise((resolve, reject) => {
      lane.waiting.push({ input, resolve, reject });
      if (lane.busy || lane.scheduled) return;
      lane.scheduled = true;
      // Deliveries dispatched from one socket read join the same batch.
      queueMicrotask(() => {
        lane.scheduled = false;
        void this.run(lane);
      });
    });
  }

  private async run(lane: Lane): Promise<void> {
    if (lane.busy) return;
    lane.busy = true;
    try {
      while (lane.waiting.length > 0) {
        await this.execute(lane.waiting.splice(0, this.options.maxBatch));
      }
    } finally {
      lane.busy = false;
    }
  }

  private async execute(batch: Submission[]): Promise<void> {
    try {
      this.admit();
    } catch (error) {
      for (const { reject } of batch) reject(error);
      return;
    }
    try {
      const outcomes = await this.store(batch.map(({ input }) => input));
      batch.forEach((submission, index) => settle(submission, outcomes[index]));
    } catch (error) {
      if (batch.length === 1 || !isolatable(error)) {
        for (const { reject } of batch) reject(error);
        return;
      }
      await this.isolate(batch);
    }
  }

  /**
   * One event can fail a whole transaction (SQLSTATE 22/23, unexpected errors). Store
   * each event alone so that only the event at fault is rejected.
   */
  private async isolate(batch: Submission[]): Promise<void> {
    for (const [index, submission] of batch.entries()) {
      try {
        this.admit();
        const [outcome] = await this.store([submission.input]);
        settle(submission, outcome);
      } catch (error) {
        if (isolatable(error)) {
          submission.reject(error);
          continue;
        }
        for (const rest of batch.slice(index)) rest.reject(error);
        return;
      }
    }
  }
}

function settle(
  submission: Submission,
  outcome: BatchOutcome | undefined,
): void {
  if (outcome === undefined) {
    submission.reject(new Error('Batch returned no outcome for the event'));
  } else if (outcome instanceof PermanentEventError) {
    submission.reject(outcome);
  } else {
    submission.resolve(outcome);
  }
}

/** Let the broker retry storage outages and lock contention without splitting the batch. */
function isolatable(error: unknown): boolean {
  const failure = classifyDatabaseFailure(error);
  return (
    failure !== DatabaseFailure.Unavailable &&
    failure !== DatabaseFailure.Contention
  );
}

/** FNV-1a: a stable, evenly spread lane for each player. */
function laneOf(playerId: string, lanes: number): number {
  let hash = 0x811c9dc5;
  for (let index = 0; index < playerId.length; index += 1) {
    hash = Math.imul(hash ^ playerId.charCodeAt(index), 0x01000193);
  }
  return (hash >>> 0) % lanes;
}
