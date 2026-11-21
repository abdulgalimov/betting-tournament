import type { BetEvent } from '../../contracts';
import { sql } from 'drizzle-orm';
import {
  check,
  index,
  jsonb,
  numeric,
  pgTable,
  timestamp,
  varchar,
} from 'drizzle-orm/pg-core';

export const bets = pgTable(
  'bets',
  {
    betId: varchar('bet_id', { length: 128 }).primaryKey(),
    playerId: varchar('player_id', { length: 128 }).notNull(),
    state: varchar('state', { length: 16 }).notNull(),
    settlement: jsonb('settlement').$type<BetEvent>(),
    settledAt: timestamp('settled_at', {
      withTimezone: true,
      precision: 6,
      mode: 'string',
    }),
    contribution: numeric('contribution', { precision: 60, scale: 12 })
      .notNull()
      .default('0'),
    exchangeRate: numeric('exchange_rate', { precision: 24, scale: 6 }),
    createdAt: timestamp('created_at', { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    check(
      'bets_state_valid',
      sql`${table.state} IN ('settled', 'rolled_back')`,
    ),
    check('bets_contribution_nonnegative', sql`${table.contribution} >= 0`),
    check(
      'bets_rolled_back_zero',
      sql`${table.state} <> 'rolled_back' OR ${table.contribution} = 0`,
    ),
    check(
      'bets_settlement_required',
      sql`${table.state} <> 'settled' OR (${table.settlement} IS NOT NULL AND ${table.exchangeRate} IS NOT NULL AND ${table.exchangeRate} > 0)`,
    ),
    check(
      'bets_settled_at_required',
      sql`${table.state} <> 'settled' OR ${table.settledAt} IS NOT NULL`,
    ),
    index('bets_active_player_settled_at_idx')
      .on(table.playerId, table.settledAt.desc())
      .where(sql`${table.state} = 'settled' AND ${table.contribution} > 0`),
  ],
);

export type BetRecord = typeof bets.$inferSelect;
