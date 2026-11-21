import {
  Inject,
  Injectable,
  ServiceUnavailableException,
} from '@nestjs/common';
import { sql } from 'drizzle-orm';
import {
  classifyDatabaseFailure,
  DatabaseFailure,
  DatabaseService,
} from '../../db';
import { leaderboardQueryDuration, RecordMetrics } from '../../observability';
import type { LeaderboardResponse } from '..';

export type { LeaderboardEntry } from '../';
export type Leaderboard = LeaderboardResponse;

@Injectable()
export class LeaderboardService {
  constructor(
    @Inject(DatabaseService) private readonly database: DatabaseService,
  ) {}

  @RecordMetrics([
    {
      metric: leaderboardQueryDuration,
    },
  ])
  async leaderboard(playerId: string): Promise<Leaderboard> {
    // One SQL statement gives the top list and personal position one MVCC snapshot.
    // The index serves the top-100 scan. The personal position adds four counts, each
    // a range of the same index (`points` first, then time and id within equal points),
    // so its cost grows with the position instead of the table size. Row comparison
    // handles equal points and times; NULL times (zero balances) sort last.
    const query = sql`
      WITH top_players AS (
        SELECT player_id, points, score_reached_at
        FROM player_scores
        ORDER BY points DESC NULLS LAST, score_reached_at ASC NULLS LAST, player_id COLLATE "C" ASC
        LIMIT 100
      ), top_ranked AS (
        SELECT player_id AS "playerId", trim_scale(points)::text AS points,
          (row_number() OVER (ORDER BY points DESC NULLS LAST, score_reached_at ASC NULLS LAST, player_id COLLATE "C" ASC))::integer AS position
        FROM top_players
      ), current_player AS (
        SELECT current.player_id AS "playerId", trim_scale(current.points)::text AS points,
          (1
            + (SELECT count(*) FROM player_scores preceding
                WHERE preceding.points > current.points)
            + (SELECT count(*) FROM player_scores preceding
                WHERE preceding.points = current.points
                  AND current.score_reached_at IS NOT NULL
                  AND (preceding.score_reached_at, preceding.player_id COLLATE "C")
                    < (current.score_reached_at, current.player_id COLLATE "C"))
            + (SELECT count(*) FROM player_scores preceding
                WHERE preceding.points = current.points
                  AND current.score_reached_at IS NULL
                  AND preceding.score_reached_at IS NOT NULL)
            + (SELECT count(*) FROM player_scores preceding
                WHERE preceding.points = current.points
                  AND current.score_reached_at IS NULL
                  AND preceding.score_reached_at IS NULL
                  AND preceding.player_id COLLATE "C" < current.player_id COLLATE "C")
          )::integer AS position
        FROM player_scores current
        WHERE current.player_id = ${playerId}
      )
      SELECT
        COALESCE((SELECT jsonb_agg(to_jsonb(top_ranked) ORDER BY position) FROM top_ranked), '[]'::jsonb) AS top,
        (SELECT to_jsonb(current_player) FROM current_player) AS player
    `;

    try {
      const result = await this.database.reader.execute<Leaderboard>(query);
      return result.rows[0]!;
    } catch (error) {
      // Pool exhaustion, database timeouts or an unreachable database: shed load fast
      // without logging driver errors, which carry SQL text and parameters.
      const failure = classifyDatabaseFailure(error);
      if (
        failure === DatabaseFailure.Unavailable ||
        failure === DatabaseFailure.Contention
      ) {
        throw new ServiceUnavailableException(
          'Leaderboard is temporarily unavailable',
        );
      }
      throw error;
    }
  }
}
