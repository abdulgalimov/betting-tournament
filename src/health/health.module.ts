import { Module } from '@nestjs/common';
import { DatabaseModule } from '../db';
import { TournamentModule } from '../tournament';
import { HealthController } from './health.controller';
import { HealthService } from './health.service';

@Module({
  imports: [DatabaseModule, TournamentModule],
  providers: [HealthService],
  controllers: [HealthController],
})
export class HealthModule {}
