import { Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { AuthController } from './auth.controller';
import { grpcClients } from './grpc-clients';
import { InteractionController } from './interaction.controller';
import { ProfileController } from './profile.controller';
import { RateLimitGuard } from './rate-limit.guard';

@Module({
  imports: [grpcClients],
  controllers: [AuthController, ProfileController, InteractionController],
  // Global guard: runs on EVERY route, before AuthGuard (so floods don't even cost a token check).
  providers: [{ provide: APP_GUARD, useClass: RateLimitGuard }],
})
export class AppModule {}
