DROP INDEX "player_scores_leaderboard_idx";--> statement-breakpoint
ALTER TABLE "bets" ADD COLUMN "settled_at" timestamp(6) with time zone;--> statement-breakpoint
ALTER TABLE "player_scores" ADD COLUMN "score_reached_at" timestamp(6) with time zone;--> statement-breakpoint
-- Legacy events contain the timestamp originally persisted in the settlement JSON.
-- Older code normalized it to milliseconds; any precision already lost cannot be recovered.
UPDATE "bets"
SET "settled_at" = ("settlement" ->> 'settledAt')::timestamptz
WHERE "settlement" IS NOT NULL;--> statement-breakpoint
UPDATE "player_scores" AS scores
SET "score_reached_at" = (
  SELECT max("settled_at")
  FROM "bets"
  WHERE "player_id" = scores."player_id"
    AND "state" = 'settled' AND "contribution" > 0
);--> statement-breakpoint
CREATE INDEX "bets_active_player_settled_at_idx" ON "bets" USING btree ("player_id","settled_at" DESC NULLS LAST) WHERE "bets"."state" = 'settled' AND "bets"."contribution" > 0;--> statement-breakpoint
CREATE INDEX "player_scores_leaderboard_idx" ON "player_scores" USING btree ("points" DESC NULLS LAST,"score_reached_at","player_id" COLLATE "C" ASC);--> statement-breakpoint
ALTER TABLE "bets" ADD CONSTRAINT "bets_settled_at_required" CHECK ("bets"."state" <> 'settled' OR "bets"."settled_at" IS NOT NULL);--> statement-breakpoint
ALTER TABLE "player_scores" ADD CONSTRAINT "player_scores_achievement_time_valid" CHECK (("player_scores"."points" = 0 AND "player_scores"."score_reached_at" IS NULL) OR ("player_scores"."points" > 0 AND "player_scores"."score_reached_at" IS NOT NULL));
