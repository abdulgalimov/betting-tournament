CREATE TABLE "bets" (
	"bet_id" varchar(128) PRIMARY KEY NOT NULL,
	"player_id" varchar(128) NOT NULL,
	"state" varchar(16) NOT NULL,
	"settlement" jsonb,
	"contribution" numeric(60, 12) DEFAULT '0' NOT NULL,
	"exchange_rate" numeric(24, 6),
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "bets_state_valid" CHECK ("bets"."state" IN ('settled', 'rolled_back')),
	CONSTRAINT "bets_contribution_nonnegative" CHECK ("bets"."contribution" >= 0),
	CONSTRAINT "bets_rolled_back_zero" CHECK ("bets"."state" <> 'rolled_back' OR "bets"."contribution" = 0),
	CONSTRAINT "bets_settlement_required" CHECK ("bets"."state" <> 'settled' OR ("bets"."settlement" IS NOT NULL AND "bets"."exchange_rate" IS NOT NULL AND "bets"."exchange_rate" > 0))
);
--> statement-breakpoint
CREATE TABLE "player_scores" (
	"player_id" varchar(128) PRIMARY KEY NOT NULL,
	"points" numeric(60, 12) DEFAULT '0' NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "player_scores_points_nonnegative" CHECK ("player_scores"."points" >= 0)
);
--> statement-breakpoint
CREATE INDEX "player_scores_leaderboard_idx" ON "player_scores" USING btree ("points" DESC NULLS LAST,"player_id" COLLATE "C" ASC);