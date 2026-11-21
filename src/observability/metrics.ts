import { metrics } from '@opentelemetry/api';

const meter = metrics.getMeter('tournament-messaging');

export const messagesReceived = meter.createCounter(
  'tournament_messages_received',
  {
    description:
      'Number of RabbitMQ message deliveries received by the consumer',
    unit: '{message}',
  },
);

export const messageProcessingDuration = meter.createHistogram(
  'tournament_message_processing_duration',
  {
    description: 'Time spent processing a RabbitMQ message delivery',
    unit: 's',
    advice: {
      explicitBucketBoundaries: [
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
      ],
    },
  },
);

export const consumerPaused = meter.createObservableGauge(
  'tournament_consumer_paused',
  {
    description:
      '1 while event consumption is paused because PostgreSQL is unavailable, otherwise 0',
  },
);

const leaderboardMeter = metrics.getMeter('tournament-leaderboard');
const leaderboardDurationAdvice = {
  explicitBucketBoundaries: [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
  ],
};

export const leaderboardQueryDuration = leaderboardMeter.createHistogram(
  'tournament_leaderboard_query_duration',
  {
    description: 'Time spent awaiting the PostgreSQL leaderboard query',
    unit: 's',
    advice: leaderboardDurationAdvice,
  },
);

export const leaderboardResponseDuration = leaderboardMeter.createHistogram(
  'tournament_leaderboard_response_duration',
  {
    description: 'Server response time for GET /leaderboard',
    unit: 's',
    advice: leaderboardDurationAdvice,
  },
);
