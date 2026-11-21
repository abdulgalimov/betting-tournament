import {
  Inject,
  Injectable,
  Logger,
  type OnModuleDestroy,
  type OnModuleInit,
} from '@nestjs/common';
import { setTimeout as sleep } from 'node:timers/promises';
import type { ObservableResult } from '@opentelemetry/api';
import {
  type AmqpConnectionManager,
  type ChannelWrapper,
  connect,
} from 'amqp-connection-manager';
import type { Channel, ConfirmChannel, ConsumeMessage } from 'amqplib';
import { BetEventSchema, EventKind, EventKindSchema } from '../../contracts';
import { AppConfig } from '../../config';
import {
  classifyDatabaseFailure,
  DatabaseFailure,
  databaseErrorCode,
} from '../../db';
import { BetEventsService } from '../services';
import { PermanentEventError } from '../errors';
import {
  consumerPaused,
  messageProcessingDuration,
  messagesReceived,
  RecordMetrics,
} from '../../observability';
import { EventBatcher } from './event-batcher';
import { previousDeliveries, resumeBackoffMs, retryBackoffMs } from './retry';
import { EventHandleOutcome } from './types';

/** Consumption was paused while the delivery waited for its storage slot. */
class DeliveryDeferredError extends Error {
  constructor() {
    super('Event consumption is paused');
    this.name = 'DeliveryDeferredError';
  }
}

@Injectable()
export class BetConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(BetConsumer.name);
  private connection?: AmqpConnectionManager;
  private wrapper?: ChannelWrapper;
  private channel?: ConfirmChannel;
  private consumerTag: string | undefined;
  private closing = false;
  private active = false;
  /** PostgreSQL is unavailable: the consumer is cancelled until a probe succeeds. */
  private paused = false;
  /** Pauses and failed probes since the last stored event; drives the probe delay. */
  private backoffStep = 0;
  private recoveryTimer: ReturnType<typeof setTimeout> | undefined;
  private resumeTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly pending = new Set<Promise<EventHandleOutcome>>();
  /** Cuts retry backoff short on shutdown so pending deliveries settle at once. */
  private readonly stopping = new AbortController();
  /**
   * Stores deliveries in shared transactions, at most `events.batchConcurrency` at once.
   * Deliveries wait here instead of in pg-pool, where waiting longer than
   * connectionTimeoutMillis would fail them and spend broker delivery attempts.
   */
  private readonly batcher: EventBatcher;
  private readonly observePaused = (result: ObservableResult) =>
    result.observe(this.paused ? 1 : 0);

  constructor(
    @Inject(AppConfig) private readonly config: AppConfig,
    @Inject(BetEventsService)
    private readonly betEventsService: BetEventsService,
  ) {
    this.batcher = new EventBatcher(
      (inputs) => this.betEventsService.handleBatch(inputs),
      {
        maxBatch: config.events.batchSize,
        concurrency: config.events.batchConcurrency,
      },
      () => {
        if (this.paused) throw new DeliveryDeferredError();
      },
    );
  }

  isReady(): boolean {
    return (
      this.active &&
      !this.paused &&
      !this.closing &&
      this.consumerTag !== undefined &&
      this.connection?.isConnected() === true
    );
  }

  async onModuleInit(): Promise<void> {
    consumerPaused.addCallback(this.observePaused);
    this.connection = connect([this.config.rabbitmq.url], {
      heartbeatIntervalInSeconds: 5,
      reconnectTimeInSeconds: 2,
      connectionOptions: { timeout: 5000 },
    });
    this.connection.on('disconnect', () => {
      this.active = false;
      clearTimeout(this.recoveryTimer);
      this.recoveryTimer = undefined;
      this.logger.warn('RabbitMQ disconnected');
    });
    this.connection.on('connectFailed', () =>
      this.logger.warn('RabbitMQ connection attempt failed'),
    );
    this.wrapper = this.connection.createChannel({
      setup: async (channel: ConfirmChannel) => {
        this.active = false;
        this.consumerTag = undefined;
        channel.on('error', () =>
          this.logger.error('RabbitMQ consumer channel error'),
        );
        channel.on('close', () => {
          this.active = false;
          this.consumerTag = undefined;
          this.scheduleChannelRecovery();
        });
        await this.assertTopology(channel);
        await channel.prefetch(this.config.rabbitmq.prefetch);
        this.channel = channel;
        // While PostgreSQL is unavailable the topology is restored but nothing is
        // consumed; the resume probe starts consumption on this channel.
        if (!this.paused) await this.consume(channel);
        this.active = true;
      },
    });
    this.wrapper.on('error', () => {
      this.active = false;
      this.logger.error('RabbitMQ channel setup failed');
    });
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.wrapper.waitForConnect(),
        new Promise<never>((_resolve, reject) => {
          timeout = setTimeout(
            () => reject(new Error('Connection timeout')),
            10000,
          );
        }),
      ]);
    } catch {
      this.closing = true;
      await this.connection.close();
      throw new Error('RabbitMQ startup timed out');
    } finally {
      clearTimeout(timeout);
    }
  }

  async onModuleDestroy(): Promise<void> {
    this.closing = true;
    this.active = false;
    this.stopping.abort();
    clearTimeout(this.recoveryTimer);
    this.recoveryTimer = undefined;
    clearTimeout(this.resumeTimer);
    this.resumeTimer = undefined;
    consumerPaused.removeCallback(this.observePaused);
    await this.stopConsuming();
    await Promise.allSettled(this.pending);
    await this.wrapper?.close();
    await this.connection?.close();
  }

  /** Consumer-owned RabbitMQ topology; publishers follow the public AMQP protocol. */
  private async assertTopology(channel: Channel): Promise<void> {
    const config = this.config.rabbitmq;

    await channel.assertExchange(config.exchange, 'topic', {
      durable: true,
    });
    await channel.assertExchange(config.deadLetterExchange, 'direct', {
      durable: true,
    });
    await channel.assertQueue(config.deadLetterQueue, {
      durable: true,
      arguments: { 'x-queue-type': 'quorum' },
    });
    await channel.bindQueue(
      config.deadLetterQueue,
      config.deadLetterExchange,
      config.deadLetterRoutingKey,
    );
    await channel.assertQueue(config.queue, {
      durable: true,
      arguments: {
        'x-queue-type': 'quorum',
        'x-delivery-limit': config.maxDeliveryAttempts,
        'x-dead-letter-exchange': config.deadLetterExchange,
        'x-dead-letter-routing-key': config.deadLetterRoutingKey,
        'x-dead-letter-strategy': 'at-least-once',
        'x-overflow': 'reject-publish',
      },
    });
    await channel.bindQueue(
      config.queue,
      config.exchange,
      EventKind.BetSettled,
    );
    await channel.bindQueue(
      config.queue,
      config.exchange,
      EventKind.BetRollback,
    );
  }

  private async consume(channel: ConfirmChannel): Promise<void> {
    const { consumerTag } = await channel.consume(
      this.config.rabbitmq.queue,
      (message) => this.processingMessage(channel, message),
      { noAck: false },
    );
    this.consumerTag = consumerTag;
  }

  private async stopConsuming(): Promise<void> {
    const channel = this.channel;
    const consumerTag = this.consumerTag;
    this.consumerTag = undefined;
    if (channel && consumerTag) {
      await channel.cancel(consumerTag).catch(() => undefined);
    }
  }

  private processingMessage(
    channel: ConfirmChannel,
    message: ConsumeMessage | null,
  ) {
    if (!message) {
      this.active = false;
      this.consumerTag = undefined;
      this.scheduleChannelRecovery();
      return;
    }
    if (this.closing || this.paused) {
      // Prefetched before the consumer was cancelled: return it untouched.
      channel.nack(message, false, true);
      return;
    }
    const work = this.handleDelivery(channel, message).catch(() => {
      this.logger.warn('Channel closed before delivery acknowledgement');
      return EventHandleOutcome.UnknownError;
    });
    this.pending.add(work);
    void work.finally(() => this.pending.delete(work));
  }

  @RecordMetrics([
    {
      metric: messagesReceived,
    },
    {
      metric: messageProcessingDuration,
      getAttributes: (outcome: EventHandleOutcome | null) => {
        return {
          outcome: outcome || EventHandleOutcome.UnknownError,
        };
      },
    },
  ])
  async handleDelivery(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    retryDelay: (previousDeliveries: number) => Promise<void> = (count) =>
      sleep(retryBackoffMs(count), undefined, {
        signal: this.stopping.signal,
      }).catch(() => undefined),
  ): Promise<EventHandleOutcome> {
    // Redrive addresses this queue through the default exchange and stores the
    // original event kind in the type property. Topic publishers use routing keys.
    const { exchange, routingKey } = message.fields;
    const rawKind =
      exchange === '' && routingKey === this.config.rabbitmq.queue
        ? message.properties.type
        : routingKey;
    const kind = EventKindSchema.safeParse(rawKind).data;
    if (message.content.length > 16384) {
      return this.rejectEvent(channel, message, kind, 'size_limit');
    }
    if (kind === undefined) {
      return this.rejectEvent(channel, message, kind, 'invalid_event_kind');
    }

    let body: unknown;
    try {
      body = JSON.parse(message.content.toString('utf8'));
    } catch {
      return this.rejectEvent(channel, message, kind, 'invalid_json');
    }
    const parsed = BetEventSchema.safeParse(body);
    if (!parsed.success) {
      return this.rejectEvent(channel, message, kind, 'invalid_contract');
    }

    let outcome: EventHandleOutcome;
    try {
      outcome = await this.batcher.submit({ kind, event: parsed.data });
      this.backoffStep = 0;
    } catch (error) {
      return this.settleFailure(channel, message, error, retryDelay, kind);
    }
    // ACK only after the commit. An ACK lost with a closed channel is safe: the broker
    // redelivers and the stored bet turns the repeat into a duplicate.
    channel.ack(message);
    return outcome;
  }

  private rejectEvent(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    kind: EventKind | undefined,
    reason:
      | 'size_limit'
      | 'invalid_event_kind'
      | 'invalid_json'
      | 'invalid_contract'
      | 'domain_error',
  ): EventHandleOutcome {
    this.logger.error(
      {
        kind,
        bodyBytes: message.content.length,
        previousDeliveries: previousDeliveries(message),
        reason,
      },
      'Event rejected',
    );
    channel.nack(message, false, false);
    return EventHandleOutcome.PermanentError;
  }

  private async settleFailure(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    error: unknown,
    retryDelay: (previousDeliveries: number) => Promise<void>,
    kind: EventKind,
  ): Promise<EventHandleOutcome> {
    if (error instanceof PermanentEventError) {
      return this.rejectEvent(channel, message, kind, 'domain_error');
    }
    if (error instanceof DeliveryDeferredError) {
      channel.nack(message, false, true);
      return EventHandleOutcome.Deferred;
    }

    // Do not log driver errors: they can contain SQL parameters or connection URLs.
    // SQLSTATE and system error codes are safe and explain the failure.
    const code = databaseErrorCode(error);
    const failure = classifyDatabaseFailure(error);
    if (failure === DatabaseFailure.Unavailable) {
      // The event is valid and storage is not: requeue it without spending a
      // delivery attempt and stop consuming until PostgreSQL answers again.
      this.pause(code);
      channel.nack(message, false, true);
      return EventHandleOutcome.InfrastructureError;
    }

    const prior = previousDeliveries(message);
    const context = {
      kind,
      bodyBytes: message.content.length,
      previousDeliveries: prior,
      ...(code === undefined ? {} : { code }),
    };
    if (failure === DatabaseFailure.InvalidData) {
      this.logger.error(
        { ...context, reason: 'invalid_data' },
        'Event rejected',
      );
      channel.nack(message, false, false);
      return EventHandleOutcome.DataError;
    }

    this.logger.error(
      { ...context, reason: failure },
      'Event transaction failed; retrying through broker delivery limit',
    );
    await retryDelay(prior);
    // RabbitMQ >=4.3 counts basic.reject failures toward quorum delivery-limit;
    // basic.nack with requeue does not increment that counter.
    channel.reject(message, true);
    return EventHandleOutcome.RetryableError;
  }

  private pause(code: string | undefined): void {
    if (this.paused || this.closing) return;
    this.paused = true;
    this.logger.error(
      code === undefined ? {} : { code },
      'PostgreSQL unavailable; event consumption paused until it recovers',
    );
    void this.stopConsuming();
    this.scheduleResume();
  }

  private scheduleResume(): void {
    if (this.closing || this.resumeTimer) return;
    const delay = resumeBackoffMs(this.backoffStep);
    this.backoffStep += 1;
    this.resumeTimer = setTimeout(() => {
      this.resumeTimer = undefined;
      void this.tryResume();
    }, delay);
    this.resumeTimer.unref();
  }

  private async tryResume(): Promise<void> {
    if (this.closing || !this.paused) return;
    try {
      await this.betEventsService.checkDatabase();
    } catch {
      this.scheduleResume();
      return;
    }
    if (this.closing) return;
    this.paused = false;
    this.logger.warn('PostgreSQL available again; resuming event consumption');
    const channel = this.channel;
    // Without a ready channel, channel recovery starts consumption after setup.
    if (!channel || !this.active || this.consumerTag) return;
    try {
      await this.consume(channel);
    } catch {
      this.logger.error(
        'Could not resume RabbitMQ consumption; waiting for channel recovery',
      );
    }
  }

  private scheduleChannelRecovery(): void {
    if (this.closing || this.recoveryTimer) return;
    // amqplib closes channels BEFORE emitting connection.close. Let the manager
    // handle that event first; forcing reconnect synchronously removes its listener.
    this.recoveryTimer = setTimeout(() => {
      this.recoveryTimer = undefined;
      if (!this.closing && !this.active && this.connection?.isConnected()) {
        this.connection.reconnect();
      }
    }, 0);
    this.recoveryTimer.unref();
  }
}
