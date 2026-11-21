import { connect, type GetMessage } from 'amqplib';
// Import the schema module directly: the config index parses the full app environment.
import { ConfigSchema } from '../config/config.schema';
import { readDeadLetter, republishOptions } from './dead-letter';
import { parseRedriveArguments, REDRIVE_USAGE } from './redrive-arguments';

const Settings = ConfigSchema.pick({
  RABBITMQ_URL: true,
  RABBITMQ_QUEUE: true,
  RABBITMQ_DEAD_LETTER_QUEUE: true,
});

const print = (line: string) => process.stdout.write(`${line}\n`);

async function run(): Promise<void> {
  const options = parseRedriveArguments(process.argv.slice(2));
  if (options.help) {
    print(REDRIVE_USAGE);
    return;
  }
  const parsed = Settings.safeParse(process.env);
  if (!parsed.success) {
    // Report field names only: URLs can contain credentials.
    throw new Error(
      `Invalid environment fields: ${parsed.error.issues.map((issue) => issue.path.join('.')).join(', ')}`,
    );
  }
  const settings = parsed.data;
  const queue = settings.RABBITMQ_DEAD_LETTER_QUEUE;
  const targetQueue = settings.RABBITMQ_QUEUE;
  if (queue === targetQueue) {
    throw new Error('The event queue and dead-letter queue must be distinct');
  }

  const connection = await connect(settings.RABBITMQ_URL, { timeout: 5_000 });
  try {
    const channel = await connection.createConfirmChannel();
    const { messageCount } = await channel.checkQueue(queue);
    const budget = Math.min(messageCount, options.limit ?? messageCount);
    print(`${queue}: ${messageCount} message(s)`);
    if (!options.apply) {
      print('Dry run: nothing moved. Add --apply to move messages.');
      return;
    }

    let unroutable = false;
    channel.on('return', () => {
      unroutable = true;
    });
    const held: GetMessage[] = [];
    const moved = new Map<string, number>();
    let skipped = 0;
    for (let index = 0; index < budget; index += 1) {
      const message = await channel.get(queue, { noAck: false });
      if (message === false) break;
      const deadLetter = readDeadLetter(message.properties, targetQueue);
      if (
        deadLetter === undefined ||
        (options.reason !== undefined && deadLetter.reason !== options.reason)
      ) {
        // Keep it unacknowledged so the next get returns another message.
        held.push(message);
        skipped += 1;
        continue;
      }
      unroutable = false;
      channel.sendToQueue(targetQueue, message.content, {
        ...republishOptions(message.properties),
        type: deadLetter.routingKey,
      });
      // The default exchange binds only the named queue for this routing key.
      // If that queue is absent, RabbitMQ returns the message before the confirm.
      await channel.waitForConfirms();
      if (unroutable) {
        held.push(message);
        skipped += 1;
        continue;
      }
      channel.ack(message);
      const key = `${deadLetter.reason} ${deadLetter.routingKey}`;
      moved.set(key, (moved.get(key) ?? 0) + 1);
    }
    for (const message of held) channel.nack(message, false, true);

    const total = [...moved.values()].reduce((sum, count) => sum + count, 0);
    print(`Moved ${total} message(s) to queue ${targetQueue}.`);
    for (const [key, count] of moved) print(`  ${key}: ${count}`);
    if (skipped > 0) {
      print(
        `Left ${skipped} message(s) in ${queue}: filtered out, unknown source or unroutable.`,
      );
    }
    await channel.close();
  } finally {
    await connection.close();
  }
}

void run().catch((error: unknown) => {
  // Do not print the error object: connection errors can include the broker URL.
  process.stderr.write(
    `Dead-letter redrive failed: ${error instanceof Error ? error.message : 'unknown error'}\n`,
  );
  process.exitCode = 1;
});
