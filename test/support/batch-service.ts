import type { BetEvent, EventKind } from '../../src/contracts';
import { PermanentEventError } from '../../src/tournament/errors';
import type {
  BatchOutcome,
  BetEventInput,
} from '../../src/tournament/services/bet-batch';

/**
 * Builds a `handleBatch` stub from a one-event stub, reporting outcomes the way the
 * service does: a domain refusal becomes that event's outcome, any other error fails
 * the whole batch.
 */
export function batchOf(
  handleEvent: (kind: EventKind, event: BetEvent) => Promise<unknown>,
) {
  return async (inputs: BetEventInput[]): Promise<BatchOutcome[]> => {
    const outcomes: BatchOutcome[] = [];
    for (const { kind, event } of inputs) {
      try {
        outcomes.push((await handleEvent(kind, event)) as BatchOutcome);
      } catch (error) {
        if (!(error instanceof PermanentEventError)) throw error;
        outcomes.push(error);
      }
    }
    return outcomes;
  };
}
