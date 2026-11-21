import { AppConfig } from '../config';
import { Environment } from '../config/config.schema';

export function createTransports(config: AppConfig) {
  const transports: {
    target: string;
    level: string;
    options: Record<string, unknown>;
  }[] = [];
  const pretty =
    process.stdout.isTTY && config.environment !== Environment.PRODUCTION;

  if (pretty && !config.logger.lokiUrl) {
    transports.push({
      target: 'pino-pretty',
      level: config.logger.level,
      options: {
        colorize: true,
        levelFirst: true,
        translateTime: 'SYS:standard',
        singleLine: true,
        ignore: 'pid,hostname',
      },
    });
  }

  if (config.logger.lokiUrl) {
    transports.push({
      target: 'pino-loki',
      level: config.logger.level,
      options: {
        host: config.logger.lokiUrl,
        labels: { service_name: 'tournament' },
      },
    });
  }

  return transports;
}
