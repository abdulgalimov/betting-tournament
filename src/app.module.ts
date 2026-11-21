import { Module } from '@nestjs/common';
import { AppConfigModule } from './config';
import { HealthModule } from './health';
import { LoggerModule } from './logger';
import { TournamentModule } from './tournament';

@Module({
  imports: [AppConfigModule, LoggerModule, TournamentModule, HealthModule],
})
export class AppModule {}
