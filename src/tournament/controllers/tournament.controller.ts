import {
  BadRequestException,
  Controller,
  Get,
  Headers,
  Inject,
} from '@nestjs/common';
import {
  ApiBadRequestResponse,
  ApiHeader,
  ApiOkResponse,
  ApiOperation,
  ApiServiceUnavailableResponse,
  ApiTags,
} from '@nestjs/swagger';
import type { SchemaObject } from '@nestjs/swagger';
import { Routes, PlayerIdSchema } from '../../contracts';
import { z } from 'zod';
import { LeaderboardResponseSchema } from '../dto';
import { LeaderboardService } from '../services';

@ApiTags('tournament')
@Controller(Routes.Leaderboard)
export class TournamentController {
  constructor(
    @Inject(LeaderboardService)
    private readonly leaderboardService: LeaderboardService,
  ) {}

  @Get()
  @ApiOperation({
    operationId: 'AppController_leaderboard',
    summary:
      'Top 100 and the current player position (one consistent snapshot)',
  })
  @ApiHeader({
    name: 'X-Player-Id',
    required: true,
    schema: { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' },
  })
  @ApiOkResponse({
    schema: z.toJSONSchema(LeaderboardResponseSchema, {
      target: 'openapi-3.0',
    }) as SchemaObject,
  })
  @ApiBadRequestResponse({ description: 'Missing or invalid X-Player-Id' })
  @ApiServiceUnavailableResponse({
    description:
      'Leaderboard database pool saturated, query timed out or database unavailable; retry later',
  })
  leaderboard(@Headers('x-player-id') playerId: unknown) {
    const parsed = PlayerIdSchema.safeParse(playerId);
    if (!parsed.success) {
      throw new BadRequestException('Invalid or missing X-Player-Id');
    }

    return this.leaderboardService.leaderboard(parsed.data);
  }
}
