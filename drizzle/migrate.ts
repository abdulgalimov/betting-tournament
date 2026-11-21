import { fileURLToPath } from 'node:url';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import pino from 'pino';
import { z } from 'zod';

const logger = pino({ name: 'tournament-migrate', level: 'error' });

async function run(): Promise<void> {
  const parsed = z
    .url()
    .refine((url) => /^postgres(?:ql)?:\/\//.test(url))
    .safeParse(process.env.DATABASE_URL);
  // Never include a rejected URL: connection strings can contain passwords.
  if (!parsed.success)
    throw new Error('Invalid environment field: DATABASE_URL');
  const pool = new Pool({
    connectionString: parsed.data,
    connectionTimeoutMillis: 5_000,
  });
  pool.on('error', (error) =>
    logger.error({ err: error }, 'Idle migration connection failed'),
  );
  try {
    const client = await pool.connect();
    try {
      // Keep the lock and migration on the same session. Concurrent migration jobs
      // wait here rather than racing to apply the same DDL.
      await client.query('SELECT pg_advisory_lock(194857312, 1)');
      await migrate(drizzle(client), {
        migrationsFolder: fileURLToPath(
          new URL('../drizzle/migrations', import.meta.url),
        ),
      });
    } finally {
      await client
        .query('SELECT pg_advisory_unlock(194857312, 1)')
        .catch(() => undefined);
      client.release();
    }
  } finally {
    await pool.end();
  }
}

void run().catch((error: unknown) => {
  logger.error({ err: error }, 'Database migration failed');
  process.exitCode = 1;
});
