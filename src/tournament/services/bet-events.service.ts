import { Inject, Injectable } from '@nestjs/common';
import { sql } from 'drizzle-orm';
import { BetEvent, EventKind } from '../../contracts';
import { AppConfig } from '../../config';
import { DatabaseService } from '../../db';
import { PermanentEventError } from '../errors';
import {
  type BatchOutcome,
  type BetEventInput,
  planBatch,
  type StoredBet,
} from './bet-batch';
import { EventOutcome } from './types';

@Injectable()
export class BetEventsService {
  constructor(
    @Inject(DatabaseService) private readonly database: DatabaseService,
    @Inject(AppConfig) private readonly config: AppConfig,
  ) {}

  /** Resolves once the pool used for event writes can run a query again. */
  async checkDatabase(): Promise<void> {
    await this.database.writerHealthCheck();
  }

  /**
   * Stores many events in one transaction with a fixed number of statements.
   * Outcomes equal processing the events one by one in the given order. A permanent
   * problem of one event is returned in its slot and does not abort the others.
   */
  async handleBatch(inputs: readonly BetEventInput[]): Promise<BatchOutcome[]> {
    if (inputs.length === 0) return [];
    const betIds = JSON.stringify([
      ...new Set(inputs.map((i) => i.event.betId)),
    ]);
    const playerIds = JSON.stringify([
      ...new Set(inputs.map((i) => i.event.playerId)),
    ]);
    return this.database.db.transaction(
      async (tx) => {
        // Every writer locks bets in ascending key order, then players in ascending
        // key order, so batches and single events cannot deadlock. Volatile output
        // expressions are evaluated after ORDER BY.
        await tx.execute(sql`
          SELECT pg_advisory_xact_lock(key)
          FROM (SELECT DISTINCT hashtextextended(id, 0) AS key
                FROM jsonb_array_elements_text(${betIds}::jsonb) AS id) AS keys
          ORDER BY key`);
        await tx.execute(sql`
          SELECT pg_advisory_xact_lock(9137, key)
          FROM (SELECT DISTINCT hashtext(id) AS key
                FROM jsonb_array_elements_text(${playerIds}::jsonb) AS id) AS keys
          ORDER BY key`);
        const stored = await tx.execute<StoredBet>(sql`
          SELECT bet_id, player_id, state, settlement, contribution::text AS contribution,
            settled_at::text AS settled_at
          FROM bets
          WHERE bet_id = ANY (ARRAY(SELECT jsonb_array_elements_text(${betIds}::jsonb)))`);

        const plan = planBatch(inputs, stored.rows, this.config.exchangeRates);

        if (plan.debits.length > 0) {
          await tx.execute(sql`
            UPDATE bets SET state = ${EventOutcome.RolledBack}, contribution = 0, updated_at = now()
            WHERE bet_id = ANY (ARRAY(
              SELECT bet_id FROM jsonb_to_recordset(${JSON.stringify(plan.debits)}::jsonb)
                AS debit(bet_id text)))`);
        }
        if (plan.newBets.length > 0) {
          await tx.execute(sql`
            INSERT INTO bets (bet_id, player_id, state, settlement, settled_at, contribution, exchange_rate)
            SELECT bet_id, player_id, state, settlement, settled_at, contribution, exchange_rate
            FROM jsonb_to_recordset(${JSON.stringify(plan.newBets)}::jsonb) AS bet(
              bet_id text, player_id text, state text, settlement jsonb,
              settled_at timestamptz, contribution numeric, exchange_rate numeric)`);
        }
        if (plan.credits.length > 0) {
          // Zero-win settlements create the participant but keep the achievement time.
          await tx.execute(sql`
            INSERT INTO player_scores (player_id, points, score_reached_at)
            SELECT player_id, sum(contribution), max(settled_at) FILTER (WHERE contribution > 0)
            FROM jsonb_to_recordset(${JSON.stringify(plan.credits)}::jsonb) AS credit(
              player_id text, contribution numeric, settled_at timestamptz)
            GROUP BY player_id
            ON CONFLICT (player_id) DO UPDATE SET
              points = player_scores.points + excluded.points,
              score_reached_at = CASE WHEN excluded.score_reached_at IS NULL
                THEN player_scores.score_reached_at
                ELSE GREATEST(player_scores.score_reached_at, excluded.score_reached_at) END,
              updated_at = now()`);
        }
        if (plan.debits.length > 0) {
          // Runs after the inserts so the recomputed time sees this batch's settlements.
          await tx.execute(sql`
            UPDATE player_scores AS scores SET
              points = scores.points - debit.total,
              score_reached_at = (
                SELECT max(settled_at) FROM bets
                WHERE player_id = scores.player_id AND state = 'settled' AND contribution > 0),
              updated_at = now()
            FROM (SELECT player_id, sum(contribution) AS total
                  FROM jsonb_to_recordset(${JSON.stringify(plan.debits)}::jsonb)
                    AS debit(player_id text, contribution numeric)
                  GROUP BY player_id) AS debit
            WHERE scores.player_id = debit.player_id`);
        }
        if (plan.instantChecks.length > 0) {
          // Compare instants inside PostgreSQL, retaining all six fractional digits.
          const changed = await tx.execute<{ index: number }>(sql`
            SELECT "index" FROM jsonb_to_recordset(${JSON.stringify(plan.instantChecks)}::jsonb)
              AS checked("index" integer, saved timestamptz, incoming timestamptz)
            WHERE saved <> incoming`);
          for (const { index } of changed.rows) {
            plan.outcomes[index] = new PermanentEventError(
              'Conflicting settlement for an existing betId',
            );
          }
        }
        return plan.outcomes;
      },
      { isolationLevel: 'read committed' },
    );
  }

  /** Stores one event; a permanent problem of the event is thrown. */
  async handleEvent(kind: EventKind, event: BetEvent): Promise<EventOutcome> {
    const [outcome] = await this.handleBatch([{ kind, event }]);
    if (outcome instanceof PermanentEventError) throw outcome;
    return outcome!;
  }
}
