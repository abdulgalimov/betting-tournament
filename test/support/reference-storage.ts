import { eq, sql } from 'drizzle-orm';
import { type BetEvent, EventKind } from '../../src/contracts';
import {
  bets,
  type BetRecord,
  type DatabaseService,
  playerScores,
  type Transaction,
} from '../../src/db';
import { PermanentEventError } from '../../src/tournament/errors';
import { EventOutcome } from '../../src/tournament/services';
import { calculatePoints, canonicalDecimal } from '../../src/tournament/utils';

/**
 * The former storage path: one transaction per event, written independently of the
 * batch code. Differential tests compare `BetEventsService.handleBatch` against it.
 */
export function storeOneByOne(
  database: DatabaseService,
  exchangeRates: Readonly<Record<string, string>>,
  kind: EventKind,
  event: BetEvent,
): Promise<EventOutcome> {
  return database.db.transaction(
    async (tx) => {
      await tx.execute(
        sql`SELECT pg_advisory_xact_lock(hashtextextended(${event.betId}, 0))`,
      );
      await tx.execute(
        sql`SELECT pg_advisory_xact_lock(9137, hashtext(${event.playerId}))`,
      );
      const [existing] = await tx
        .select()
        .from(bets)
        .where(eq(bets.betId, event.betId));

      if (existing && existing.playerId !== event.playerId) {
        throw new PermanentEventError(
          'betId already belongs to a different player',
        );
      }
      switch (kind) {
        case EventKind.BetRollback:
          return rollback(tx, event, existing);
        case EventKind.BetSettled:
          return settle(tx, exchangeRates, event, existing);
      }
      throw new PermanentEventError(`Unknown event kind: ${kind}`);
    },
    { isolationLevel: 'read committed' },
  );
}

function canonicalSettlement(event: BetEvent): BetEvent {
  return {
    betId: event.betId,
    playerId: event.playerId,
    currency: event.currency,
    stake: canonicalDecimal(event.stake),
    winAmount: canonicalDecimal(event.winAmount),
    settledAt: event.settledAt,
  };
}

async function rollback(
  tx: Transaction,
  event: BetEvent,
  existing: BetRecord | undefined,
): Promise<EventOutcome> {
  if (existing?.state === EventOutcome.RolledBack) {
    return EventOutcome.Duplicate;
  }
  if (!existing) {
    await tx.insert(bets).values({
      betId: event.betId,
      playerId: event.playerId,
      state: EventOutcome.RolledBack,
    });
    return EventOutcome.RolledBack;
  }
  await tx
    .update(bets)
    .set({
      state: EventOutcome.RolledBack,
      contribution: '0',
      updatedAt: sql`now()`,
    })
    .where(eq(bets.betId, event.betId));
  await tx
    .update(playerScores)
    .set({
      points: sql`${playerScores.points} - ${existing.contribution}`,
      scoreReachedAt: sql`(
        SELECT max(settled_at) FROM bets
        WHERE player_id = ${existing.playerId}
          AND state = 'settled' AND contribution > 0
      )`,
      updatedAt: sql`now()`,
    })
    .where(eq(playerScores.playerId, existing.playerId));
  return EventOutcome.RolledBack;
}

async function settle(
  tx: Transaction,
  exchangeRates: Readonly<Record<string, string>>,
  event: BetEvent,
  existing: BetRecord | undefined,
): Promise<EventOutcome> {
  if (existing?.state === EventOutcome.RolledBack) return EventOutcome.Ignored;
  const settlement = canonicalSettlement(event);
  if (existing) {
    const saved = canonicalSettlement(existing.settlement!);
    if (
      saved.betId !== settlement.betId ||
      saved.playerId !== settlement.playerId ||
      saved.currency !== settlement.currency ||
      saved.stake !== settlement.stake ||
      saved.winAmount !== settlement.winAmount
    ) {
      throw new PermanentEventError(
        'Conflicting settlement for an existing betId',
      );
    }
    const sameTime = await tx.execute<{ sameInstant: boolean }>(sql`
      SELECT settled_at = ${settlement.settledAt}::timestamptz AS "sameInstant"
      FROM bets WHERE bet_id = ${event.betId}
    `);
    if (sameTime.rows[0]?.sameInstant !== true) {
      throw new PermanentEventError(
        'Conflicting settlement for an existing betId',
      );
    }
    return EventOutcome.Duplicate;
  }

  const rate = exchangeRates[event.currency];
  if (rate === undefined) {
    throw new PermanentEventError(`Unsupported currency: ${event.currency}`);
  }
  const contribution = calculatePoints(event.winAmount, rate);
  await tx.insert(bets).values({
    betId: event.betId,
    playerId: event.playerId,
    state: EventOutcome.Settled,
    settlement,
    settledAt: event.settledAt,
    contribution,
    exchangeRate: rate,
  });
  await tx
    .insert(playerScores)
    .values({
      playerId: event.playerId,
      points: contribution,
      scoreReachedAt: contribution === '0' ? null : event.settledAt,
    })
    .onConflictDoUpdate({
      target: playerScores.playerId,
      set: {
        points: sql`${playerScores.points} + ${contribution}`,
        scoreReachedAt: sql`CASE WHEN ${contribution}::numeric > 0
          THEN GREATEST(${playerScores.scoreReachedAt}, ${event.settledAt}::timestamptz)
          ELSE ${playerScores.scoreReachedAt} END`,
        updatedAt: sql`now()`,
      },
    });
  return EventOutcome.Settled;
}
