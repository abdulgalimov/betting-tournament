import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { drizzle, type NodePgDatabase } from 'drizzle-orm/node-postgres';
import { Pool, type PoolConfig } from 'pg';
import { AppConfig } from '../config';
import { bets, playerScores } from './entities';

const schema = { bets, playerScores };

@Injectable()
export class DatabaseService implements OnApplicationShutdown {
  private readonly logger = new Logger(DatabaseService.name);
  /** Event writes: advisory-locked transactions, sized for the consumer. */
  readonly pool: Pool;
  readonly db: NodePgDatabase<typeof schema>;
  /**
   * HTTP reads use their own pool so leaderboard load cannot take connections from
   * event processing. Short timeouts turn overload into a fast 503 instead of a queue.
   */
  readonly readerPool: Pool;
  readonly reader: NodePgDatabase<typeof schema>;

  constructor(@Inject(AppConfig) private readonly config: AppConfig) {
    this.pool = this.createPool('Event', {
      connectionString: config.database.url,
      max: config.database.maxPool,
      connectionTimeoutMillis: 5_000,
      idleTimeoutMillis: 30_000,
      statement_timeout: 10_000,
      // Waits for advisory or row locks end well before statement_timeout.
      lock_timeout: 5_000,
      idle_in_transaction_session_timeout: 30_000,
      application_name: 'tournament',
    });
    this.readerPool = this.createPool('Leaderboard', {
      connectionString: config.database.url,
      max: config.database.http.maxPool,
      connectionTimeoutMillis: config.database.http.connectionTimeoutMs,
      idleTimeoutMillis: 30_000,
      statement_timeout: config.database.http.statementTimeoutMs,
      application_name: 'tournament-http',
    });
    this.db = drizzle(this.pool, { schema });
    this.reader = drizzle(this.readerPool, { schema });
  }

  /** Validate this instance's connection budget before starting HTTP or AMQP. */
  async validatePoolSize(): Promise<void> {
    const result = await this.pool.query<{
      max_connections: string;
      superuser_reserved_connections: string;
      reserved_connections: string;
    }>(`
      SELECT current_setting('max_connections') AS max_connections,
             current_setting('superuser_reserved_connections') AS superuser_reserved_connections,
             COALESCE(current_setting('reserved_connections', true), '0') AS reserved_connections
    `);
    const settings = result.rows[0];
    const maxConnections = Number(settings?.max_connections ?? NaN);
    const superuserReservedConnections = Number(
      settings?.superuser_reserved_connections ?? NaN,
    );
    const reservedConnections = Number(settings?.reserved_connections ?? NaN);
    // Keep the reserves available even when the application's role can use them.
    const availableConnections =
      maxConnections - superuserReservedConnections - reservedConnections;

    if (
      !Number.isInteger(maxConnections) ||
      maxConnections <= 0 ||
      !Number.isInteger(superuserReservedConnections) ||
      superuserReservedConnections < 0 ||
      !Number.isInteger(reservedConnections) ||
      reservedConnections < 0 ||
      availableConnections <= 0
    ) {
      throw new Error(
        'Cannot validate PostgreSQL pool sizes: invalid connection settings response',
      );
    }

    const { maxPool, http } = this.config.database;
    const total = maxPool + http.maxPool;
    if (total > availableConnections) {
      const message =
        `PostgreSQL pool sizes exceed available connections: ` +
        `DATABASE_MAX_POOL=${maxPool}, DATABASE_HTTP_MAX_POOL=${http.maxPool}, ` +
        `total=${total}, max_connections=${maxConnections}, ` +
        `superuser_reserved_connections=${superuserReservedConnections}, ` +
        `reserved_connections=${reservedConnections}, ` +
        `available_connections=${availableConnections}. ` +
        'Reduce the configured pool sizes.';
      this.logger.error(message);
      throw new Error(message);
    }
  }

  /** Readiness of the HTTP read path. */
  async healthCheck(): Promise<void> {
    await this.readerPool.query('SELECT 1');
  }

  /** Probe used by the consumer before it resumes after PostgreSQL was unavailable. */
  async writerHealthCheck(): Promise<void> {
    await this.pool.query('SELECT 1');
  }

  async onApplicationShutdown(): Promise<void> {
    await Promise.all([this.pool.end(), this.readerPool.end()]);
  }

  private createPool(name: string, options: PoolConfig): Pool {
    const pool = new Pool(options);
    // Idle connection failures must be observed rather than emitted unhandled.
    pool.on('error', (error) =>
      this.logger.error(
        { err: error },
        `Idle PostgreSQL connection failed (${name} pool)`,
      ),
    );
    return pool;
  }
}
