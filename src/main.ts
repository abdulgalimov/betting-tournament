import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { AppConfig } from './config';
import { DatabaseService } from './db';
import { registerRouteMetrics } from './observability';
import { setupSwagger } from './swagger';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    bufferLogs: true,
    bodyParser: false,
  });
  try {
    const logger = app.get(Logger);
    registerRouteMetrics(app);

    app.useLogger(logger);
    app.useBodyParser('json', { limit: '16kb' });
    app.enableShutdownHooks();

    await app.get(DatabaseService).validatePoolSize();

    setupSwagger(app);

    const port = app.get(AppConfig).port;
    await app.listen(port, '0.0.0.0');
  } catch (error) {
    await app.close();
    throw error;
  }
}

void bootstrap().catch((error) => {
  process.stderr.write(
    `Tournament startup failed; check dependency readiness and environment configuration.
${String(error)}\n`,
  );
  process.exitCode = 1;
});
