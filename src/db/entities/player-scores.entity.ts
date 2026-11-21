import { sql } from 'drizzle-orm';
import {
  check,
  index,
  numeric,
  pgTable,
  timestamp,
  varchar,
} from 'drizzle-orm/pg-core';

export const playerScores = pgTable(
  'player_scores',
  {
    playerId: varchar('player_id', { length: 128 }).primaryKey(),
    points: numeric('points', { precision: 60, scale: 12 })
      .notNull()
      .default('0'),
    scoreReachedAt: timestamp('score_reached_at', {
      withTimezone: true,
      precision: 6,
      mode: 'string',
    }),
    updatedAt: timestamp('updated_at', { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    check('player_scores_points_nonnegative', sql`${table.points} >= 0`),
    check(
      'player_scores_achievement_time_valid',
      sql`(${table.points} = 0 AND ${table.scoreReachedAt} IS NULL) OR (${table.points} > 0 AND ${table.scoreReachedAt} IS NOT NULL)`,
    ),
    index('player_scores_leaderboard_idx').on(
      table.points.desc(),
      table.scoreReachedAt.asc(),
      sql`${table.playerId} COLLATE "C" ASC`,
    ),
  ],
);
