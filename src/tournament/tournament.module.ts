import { Module } from '@nestjs/common';
import { AppConfigModule } from '../config';
import { DatabaseModule } from '../db';
import { BetEventsService, LeaderboardService } from './services';
import { BetConsumer } from './messaging';
import { TournamentController } from './controllers';

@Module({
  imports: [AppConfigModule, DatabaseModule],
  controllers: [TournamentController],
  providers: [BetEventsService, LeaderboardService, BetConsumer],
  exports: [BetConsumer],
})
export class TournamentModule {}
