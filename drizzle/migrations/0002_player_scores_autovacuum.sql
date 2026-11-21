-- Every settlement and rollback updates one player row and moves it in
-- player_scores_leaderboard_idx, so HOT updates are impossible and the visibility map
-- goes stale quickly. The personal position is counted with index-only scans, which
-- fall back to heap fetches on pages that autovacuum has not processed yet: on 1M rows
-- with 5% of them updated, the count slowed down about fourfold until VACUUM ran.
-- Vacuum this table after 1% of changes instead of the default 20%.
ALTER TABLE "player_scores" SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_insert_scale_factor = 0.01,
  autovacuum_vacuum_cost_limit = 2000
);
