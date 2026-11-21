import { PlayerIdSchema, PointsSchema } from '../contracts';
import { z } from 'zod';

export const LeaderboardEntrySchema = z.strictObject({
  playerId: PlayerIdSchema,
  points: PointsSchema,
  position: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
});
export type LeaderboardEntry = z.infer<typeof LeaderboardEntrySchema>;

export const LeaderboardResponseSchema = z.strictObject({
  top: z.array(LeaderboardEntrySchema).max(100),
  player: LeaderboardEntrySchema.nullable(),
});
export type LeaderboardResponse = z.infer<typeof LeaderboardResponseSchema>;
