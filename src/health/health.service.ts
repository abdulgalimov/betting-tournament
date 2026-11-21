import {
  Inject,
  Injectable,
  ServiceUnavailableException,
} from '@nestjs/common';
import { DatabaseService } from '../db';
import { BetConsumer } from '../tournament/messaging';

@Injectable()
export class HealthService {
  constructor(
    @Inject(DatabaseService) private readonly database: DatabaseService,
    @Inject(BetConsumer) private readonly consumer: BetConsumer,
  ) {}

  live() {
    return { status: 'ok' };
  }

  async ready() {
    try {
      await this.database.healthCheck();
      if (!this.consumer.isReady()) throw new Error('Consumer unavailable');
      return { status: 'ok' };
    } catch {
      throw new ServiceUnavailableException('Dependencies unavailable');
    }
  }
}
