import { type BetEvent, EventKind } from '../../contracts';
import { PermanentEventError } from '../errors';
import { calculatePoints, canonicalDecimal } from '../utils';
import { EventOutcome } from './types';

export interface BetEventInput {
  kind: EventKind;
  event: BetEvent;
}

/** Per-event result of a batch: an outcome, or the reason to dead-letter the event. */
export type BatchOutcome = EventOutcome | PermanentEventError;

/** A `bets` row as read inside the batch transaction. */
export type StoredBet = {
  bet_id: string;
  player_id: string;
  state: string;
  settlement: BetEvent | null;
  contribution: string;
  settled_at: string | null;
};

export interface NewBetRow {
  bet_id: string;
  player_id: string;
  state: 'settled' | 'rolled_back';
  settlement: BetEvent | null;
  settled_at: string | null;
  contribution: string;
  exchange_rate: string | null;
}

/** One new settlement of the batch; PostgreSQL sums them per player exactly. */
export interface PlayerCredit {
  player_id: string;
  contribution: string;
  settled_at: string;
}

/** A stored settled bet rolled back by the batch: its saved contribution is subtracted. */
export interface PlayerDebit {
  bet_id: string;
  player_id: string;
  contribution: string;
}

/** A repeated settlement whose instant PostgreSQL must compare with the saved one. */
export interface InstantCheck {
  index: number;
  saved: string;
  incoming: string;
}

export interface BatchPlan {
  outcomes: BatchOutcome[];
  newBets: NewBetRow[];
  credits: PlayerCredit[];
  debits: PlayerDebit[];
  instantChecks: InstantCheck[];
}

interface BetState {
  playerId: string;
  state: 'settled' | 'rolled_back';
  settlement: BetEvent | null;
  settledAt: string | null;
  contribution: string;
  exchangeRate: string | null;
  stored: boolean;
}

export function canonicalSettlement(event: BetEvent): BetEvent {
  return {
    betId: event.betId,
    playerId: event.playerId,
    currency: event.currency,
    stake: canonicalDecimal(event.stake),
    winAmount: canonicalDecimal(event.winAmount),
    // Preserve the complete timestamp: JavaScript Date truncates microseconds.
    settledAt: event.settledAt,
  };
}

const sameSettlement = (saved: BetEvent, incoming: BetEvent) =>
  saved.betId === incoming.betId &&
  saved.playerId === incoming.playerId &&
  saved.currency === incoming.currency &&
  saved.stake === incoming.stake &&
  saved.winAmount === incoming.winAmount;

/**
 * Applies a batch of events, in delivery order, to the stored state of their bets.
 * The result equals processing the events one by one: a rollback stays final, a
 * repeated settlement is a duplicate or a conflict, and the first settlement fixes
 * the exchange rate and the contribution.
 */
export function planBatch(
  inputs: readonly BetEventInput[],
  stored: readonly StoredBet[],
  exchangeRates: Readonly<Record<string, string>>,
): BatchPlan {
  const bets = new Map<string, BetState>();
  for (const row of stored) {
    bets.set(row.bet_id, {
      playerId: row.player_id,
      state: row.state === EventOutcome.RolledBack ? 'rolled_back' : 'settled',
      settlement: row.settlement ? canonicalSettlement(row.settlement) : null,
      settledAt: row.settled_at,
      contribution: row.contribution,
      exchangeRate: null,
      stored: true,
    });
  }

  const outcomes: BatchOutcome[] = [];
  const debits: PlayerDebit[] = [];
  const instantChecks: InstantCheck[] = [];

  inputs.forEach(({ kind, event }, index) => {
    const bet = bets.get(event.betId);
    if (bet && bet.playerId !== event.playerId) {
      outcomes.push(
        new PermanentEventError('betId already belongs to a different player'),
      );
      return;
    }

    if (kind === EventKind.BetRollback) {
      if (!bet) {
        // A tombstone keeps a late settlement from resurrecting this bet.
        bets.set(event.betId, {
          playerId: event.playerId,
          state: 'rolled_back',
          settlement: null,
          settledAt: null,
          contribution: '0',
          exchangeRate: null,
          stored: false,
        });
        outcomes.push(EventOutcome.RolledBack);
        return;
      }
      if (bet.state === 'rolled_back') {
        outcomes.push(EventOutcome.Duplicate);
        return;
      }
      if (bet.stored) {
        debits.push({
          bet_id: event.betId,
          player_id: bet.playerId,
          contribution: bet.contribution,
        });
      }
      bet.state = 'rolled_back';
      bet.contribution = '0';
      outcomes.push(EventOutcome.RolledBack);
      return;
    }

    if (bet?.state === 'rolled_back') {
      outcomes.push(EventOutcome.Ignored);
      return;
    }
    const settlement = canonicalSettlement(event);
    if (bet) {
      if (!bet.settlement || !sameSettlement(bet.settlement, settlement)) {
        outcomes.push(
          new PermanentEventError(
            'Conflicting settlement for an existing betId',
          ),
        );
        return;
      }
      instantChecks.push({
        index,
        saved: bet.settledAt!,
        incoming: event.settledAt,
      });
      outcomes.push(EventOutcome.Duplicate);
      return;
    }

    const rate = exchangeRates[event.currency];
    if (rate === undefined) {
      outcomes.push(
        new PermanentEventError(`Unsupported currency: ${event.currency}`),
      );
      return;
    }
    bets.set(event.betId, {
      playerId: event.playerId,
      state: 'settled',
      settlement,
      settledAt: event.settledAt,
      contribution: calculatePoints(event.winAmount, rate),
      exchangeRate: rate,
      stored: false,
    });
    outcomes.push(EventOutcome.Settled);
  });

  const newBets: NewBetRow[] = [];
  const credits: PlayerCredit[] = [];
  for (const [betId, bet] of bets) {
    if (bet.stored) continue;
    newBets.push({
      bet_id: betId,
      player_id: bet.playerId,
      state: bet.state,
      settlement: bet.settlement,
      settled_at: bet.settledAt,
      contribution: bet.contribution,
      exchange_rate: bet.exchangeRate,
    });
    // A settlement rolled back in the same batch still makes the player a participant
    // with a zero contribution, exactly as two separate transactions would.
    if (bet.settlement && bet.settledAt) {
      credits.push({
        player_id: bet.playerId,
        contribution: bet.contribution,
        settled_at: bet.settledAt,
      });
    }
  }

  return { outcomes, newBets, credits, debits, instantChecks };
}
