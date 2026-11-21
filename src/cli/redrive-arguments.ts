export const REDRIVE_USAGE = `Usage: node dist/redrive-dlq.js [--apply] [--limit N] [--reason REASON]

Moves dead-lettered bet events directly to the tournament queue, carrying the
original event kind in the AMQP type property.

  --apply          Move messages. Without it only the dead-letter queue size is shown.
  --limit N        Move at most N messages (default: all messages present at start).
  --reason REASON  Only move messages that died for REASON: delivery_limit
                   (retries exhausted) or rejected (dead-lettered at once).

Reprocessing is safe: storage is idempotent and a rollback stays final. Fix the cause
first (for example, add a missing currency to EXCHANGE_RATES) or the events return to
the dead-letter queue.`;

export interface RedriveArguments {
  apply: boolean;
  limit: number | undefined;
  reason: 'delivery_limit' | 'rejected' | undefined;
  help: boolean;
}

export function parseRedriveArguments(
  argv: readonly string[],
): RedriveArguments {
  const parsed: RedriveArguments = {
    apply: false,
    limit: undefined,
    reason: undefined,
    help: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv.at(index);
    if (argument === '--apply') {
      parsed.apply = true;
    } else if (argument === '--help' || argument === '-h') {
      parsed.help = true;
    } else if (argument === '--limit') {
      index += 1;
      const value = argv.at(index) ?? '';
      if (!/^[1-9]\d*$/.test(value)) {
        throw new Error('--limit expects a positive integer');
      }
      parsed.limit = Number(value);
    } else if (argument === '--reason') {
      index += 1;
      const value = argv.at(index);
      if (value !== 'delivery_limit' && value !== 'rejected') {
        throw new Error('--reason expects delivery_limit or rejected');
      }
      parsed.reason = value;
    } else {
      throw new Error(`Unknown argument: ${argument}`);
    }
  }
  return parsed;
}
