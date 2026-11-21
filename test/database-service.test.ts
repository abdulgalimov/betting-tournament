import { Logger } from '@nestjs/common';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { parseConfig } from '../src/config';
import { DatabaseService } from '../src/db';

type ReservedSettings = {
  superuser_reserved_connections: string | undefined;
  reserved_connections: string | undefined;
};

describe('PostgreSQL startup pool validation', () => {
  const databases: DatabaseService[] = [];

  function databaseWithLimit(
    maxConnections: string | undefined,
    writerPool = 16,
    httpPool = 8,
    reservedSettings: Partial<ReservedSettings> = {},
  ) {
    const database = new DatabaseService(
      parseConfig({
        DATABASE_URL: 'postgresql://user:startup-secret@localhost/db',
        RABBITMQ_URL: 'amqp://localhost',
        EXCHANGE_RATES: 'USD=1',
        DATABASE_MAX_POOL: String(writerPool),
        DATABASE_HTTP_MAX_POOL: String(httpPool),
      }),
    );
    databases.push(database);
    const query = vi.spyOn(database.pool, 'query');
    query.mockImplementation(
      vi.fn().mockResolvedValue({
        rows:
          maxConnections === undefined
            ? []
            : [
                {
                  max_connections: maxConnections,
                  superuser_reserved_connections: '3',
                  reserved_connections: '0',
                  ...reservedSettings,
                },
              ],
      }),
    );
    return { database, query };
  }

  beforeEach(() => {
    vi.spyOn(Logger.prototype, 'error').mockImplementation(() => {});
  });

  afterEach(async () => {
    await Promise.all(
      databases.splice(0).map((database) => database.onApplicationShutdown()),
    );
    vi.restoreAllMocks();
  });

  it.each([16, 117])(
    'accepts writer pool %s when the combined pools fit the unreserved budget',
    async (writerPool) => {
      const { database, query } = databaseWithLimit('128', writerPool);

      await expect(database.validatePoolSize()).resolves.toBeUndefined();
      expect(query).toHaveBeenCalledWith(
        expect.stringContaining(
          "current_setting('reserved_connections', true)",
        ),
      );
      expect(Logger.prototype.error).not.toHaveBeenCalled();
    },
  );

  it('rejects a writer pool of 256 against max_connections=128 with safe diagnostics', async () => {
    const { database } = databaseWithLimit('128', 256);
    const validation = database.validatePoolSize();

    for (const detail of [
      'DATABASE_MAX_POOL=256',
      'DATABASE_HTTP_MAX_POOL=8',
      'total=264',
      'max_connections=128',
      'superuser_reserved_connections=3',
      'reserved_connections=0',
      'available_connections=125',
    ]) {
      await expect(validation).rejects.toThrow(detail);
    }
    expect(Logger.prototype.error).toHaveBeenCalledOnce();
    expect(
      JSON.stringify(vi.mocked(Logger.prototype.error).mock.calls),
    ).not.toContain('startup-secret');
  });

  it('rejects combined pools that exceed the limit even when each fits individually', async () => {
    const { database } = databaseWithLimit('128', 120, 16);

    await expect(database.validatePoolSize()).rejects.toThrow('total=136');
  });

  it('rejects pools below max_connections when they consume superuser reserved slots', async () => {
    const { database } = databaseWithLimit('128', 118);

    await expect(database.validatePoolSize()).rejects.toThrow(
      'available_connections=125',
    );
  });

  it('excludes additional reserved connections from the available budget', async () => {
    const { database } = databaseWithLimit('128', 113, 8, {
      reserved_connections: '5',
    });

    await expect(database.validatePoolSize()).rejects.toThrow(
      'available_connections=120',
    );
  });

  it('accepts equality after excluding both kinds of reserved connections', async () => {
    const { database } = databaseWithLimit('128', 112, 8, {
      reserved_connections: '5',
    });

    await expect(database.validatePoolSize()).resolves.toBeUndefined();
  });

  it('allows the full connection budget when both reserves are zero', async () => {
    const { database } = databaseWithLimit('128', 120, 8, {
      superuser_reserved_connections: '0',
    });

    await expect(database.validatePoolSize()).resolves.toBeUndefined();
  });

  it.each([undefined, 'broken', '0', '1.5'])(
    'rejects an invalid max_connections response %s',
    async (maxConnections) => {
      const { database } = databaseWithLimit(maxConnections);

      await expect(database.validatePoolSize()).rejects.toThrow(
        'Cannot validate PostgreSQL pool sizes: invalid connection settings response',
      );
    },
  );

  it.each<Partial<ReservedSettings>>([
    { superuser_reserved_connections: undefined },
    { superuser_reserved_connections: 'broken' },
    { superuser_reserved_connections: '-1' },
    { superuser_reserved_connections: '1.5' },
    { reserved_connections: undefined },
    { reserved_connections: 'broken' },
    { reserved_connections: '-1' },
    { reserved_connections: '1.5' },
    { superuser_reserved_connections: '128' },
    { superuser_reserved_connections: '129' },
    { reserved_connections: '125' },
  ])(
    'rejects invalid reserved connection settings %j',
    async (reservedSettings) => {
      const { database } = databaseWithLimit('128', 16, 8, reservedSettings);

      await expect(database.validatePoolSize()).rejects.toThrow(
        'Cannot validate PostgreSQL pool sizes: invalid connection settings response',
      );
    },
  );

  it('fails startup validation when PostgreSQL cannot be queried', async () => {
    const { database, query } = databaseWithLimit('128');
    const error = Object.assign(new Error('connect ECONNREFUSED'), {
      code: 'ECONNREFUSED',
    });
    query.mockImplementation(vi.fn().mockRejectedValue(error));

    await expect(database.validatePoolSize()).rejects.toBe(error);
    expect(Logger.prototype.error).not.toHaveBeenCalled();
  });
});
