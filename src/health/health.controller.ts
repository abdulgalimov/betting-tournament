import { Controller, Get, Inject } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { HealthService } from './health.service';

@ApiTags('tournament')
@Controller('health')
export class HealthController {
  constructor(@Inject(HealthService) private readonly health: HealthService) {}

  @Get('live')
  @ApiOperation({
    summary: 'Process liveness',
    operationId: 'AppController_live',
  })
  live() {
    return this.health.live();
  }

  @Get('ready')
  @ApiOperation({
    summary: 'PostgreSQL and RabbitMQ readiness',
    operationId: 'AppController_ready',
  })
  ready() {
    return this.health.ready();
  }
}
