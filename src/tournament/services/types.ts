export const EventOutcome = {
  Settled: 'settled',
  RolledBack: 'rolled_back',
  Duplicate: 'duplicate',
  Ignored: 'ignored',
} as const;
export type EventOutcome = (typeof EventOutcome)[keyof typeof EventOutcome];
