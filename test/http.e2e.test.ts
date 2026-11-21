import { describe, expect, it } from 'vitest';

const baseUrl = process.env.TOURNAMENT_URL ?? 'http://localhost:3000';

describe('standalone tournament HTTP API', () => {
  it('serves readiness and the OpenAPI document without a producer', async () => {
    const ready = await fetch(`${baseUrl}/health/ready`, {
      signal: AbortSignal.timeout(5000),
    });
    expect(ready.status).toBe(200);
    const openapi = await fetch(`${baseUrl}/openapi.json`, {
      signal: AbortSignal.timeout(5000),
    });
    expect(openapi.status).toBe(200);
    const document = (await openapi.json()) as {
      paths: Record<string, unknown>;
    };
    expect(document.paths).toHaveProperty('/leaderboard');
  });

  it('rejects missing and invalid player identifiers', async () => {
    for (const headers of [{}, { 'X-Player-Id': 'invalid player' }]) {
      const response = await fetch(`${baseUrl}/leaderboard`, {
        headers,
        signal: AbortSignal.timeout(5000),
      });
      expect(response.status).toBe(400);
    }
  });
});
