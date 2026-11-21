import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  dialect: 'postgresql',
  schema: './src/db/entities/*.entity.ts',
  out: './drizzle/migrations',
  dbCredentials: {
    url:
      process.env.DATABASE_URL ??
      'postgresql://tournament:tournament@localhost:5432/tournament',
  },
  strict: true,
  verbose: true,
});
