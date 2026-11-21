import { NestExpressApplication } from '@nestjs/platform-express';
import { leaderboardResponseDuration } from './metrics';
import { Routes } from '../contracts';

export function registerRouteMetrics(app: NestExpressApplication) {
  const express = app.getHttpAdapter().getInstance();
  express.get(`/${Routes.Leaderboard}`, (request, response, next) => {
    if (request.method === 'GET') {
      const startedAt = performance.now();
      response.once('finish', () => {
        const executeTime = (performance.now() - startedAt) / 1000;
        leaderboardResponseDuration.record(executeTime);
      });
    }
    next();
  });
}
