import { defineConfig } from 'vitest/config';

if (!process.env.TEST_DATABASE_URL || !process.env.TEST_RABBITMQ_URL) {
  throw new Error(
    'Integration tests require TEST_DATABASE_URL and TEST_RABBITMQ_URL; copy .env.example to .env or set both variables.',
  );
}

export default defineConfig({
  test: {
    include: ['test/**/*.integration.test.ts'],
    testTimeout: 20000,
    hookTimeout: 30000,
    fileParallelism: false,
  },
});
