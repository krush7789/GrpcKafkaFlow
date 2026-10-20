import { Controller, Get, Inject, OnModuleInit, Param, Post, Req, UseGuards } from '@nestjs/common';
import { ClientGrpc } from '@nestjs/microservices';
import { AuthGuard } from './auth.guard';
import { grpcCall } from './grpc-call';
import { INTERACTION, InteractionService } from './grpc-clients';

@Controller('interactions')
@UseGuards(AuthGuard)
export class InteractionController implements OnModuleInit {
  private interactions: InteractionService;

  constructor(@Inject(INTERACTION) private readonly client: ClientGrpc) {}

  onModuleInit() {
    this.interactions = this.client.getService<InteractionService>('InteractionService');
  }

  // POST /interactions/follow/:userId  ->  gRPC InteractionService.Follow
  @Post('follow/:userId')
  follow(@Req() req: any, @Param('userId') userId: string) {
    return grpcCall(this.interactions.follow({ followerId: req.userId, followeeId: userId }));
  }

  // GET /interactions/following  ->  gRPC InteractionService.GetFollowing
  @Get('following')
  following(@Req() req: any) {
    return grpcCall(this.interactions.getFollowing({ userId: req.userId }));
  }
}
