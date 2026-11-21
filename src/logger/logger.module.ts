import { randomUUID } from 'node:crypto';
import { Module } from '@nestjs/common';
import { LoggerModule as PinoLoggerModule } from 'nestjs-pino';
import { AppConfigModule, AppConfig } from '../config';
import { createTransports } from './transports';

@Module({
  imports: [
    PinoLoggerModule.forRootAsync({
      imports: [AppConfigModule],
      inject: [AppConfig],
      useFactory: (config: AppConfig) => {
        const transports = createTransports(config);

        const transport =
          transports.length > 0 ? { targets: transports } : undefined;

        return {
          pinoHttp: {
            level: config.logger.level,
            ...(transport ? { transport } : {}),

            genReqId: (_request, response) => {
              const id = randomUUID();
              response.setHeader('X-Request-Id', id);
              return id;
            },
            redact: [
              'req.headers.authorization',
              'req.headers.cookie',
              'res.headers["set-cookie"]',
            ],
            customLogLevel: (_request, response, error) =>
              error || response.statusCode >= 500 ? 'error' : 'silent',
            autoLogging: {
              ignore: (request) => request.url?.startsWith('/health/') === true,
            },
          },
        };
      },
    }),
  ],
})
export class LoggerModule {}
