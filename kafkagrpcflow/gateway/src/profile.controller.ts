import { Body, Controller, Get, Inject, OnModuleInit, Patch, Req, UseGuards } from '@nestjs/common';
import { ClientGrpc } from '@nestjs/microservices';
import { AuthGuard } from './auth.guard';
import { grpcCall } from './grpc-call';
import { PROFILE, ProfileService } from './grpc-clients';

@Controller('profiles')
@UseGuards(AuthGuard)
export class ProfileController implements OnModuleInit {
  private profiles: ProfileService;

  constructor(@Inject(PROFILE) private readonly client: ClientGrpc) {}

  onModuleInit() {
    this.profiles = this.client.getService<ProfileService>('ProfileService');
  }

  // GET /profiles  ->  gRPC ProfileService.ListProfiles
  @Get()
  async list() {
    return (await grpcCall(this.profiles.listProfiles({}))).profiles;
  }

  // GET /profiles/me  ->  gRPC ProfileService.GetProfile
  @Get('me')
  me(@Req() req: any) {
    return grpcCall(this.profiles.getProfile({ userId: req.userId }));
  }

  // PATCH /profiles/me  { bio }  ->  gRPC ProfileService.UpdateProfile
  @Patch('me')
  update(@Req() req: any, @Body() body: { bio?: string }) {
    return grpcCall(this.profiles.updateProfile({ userId: req.userId, bio: body?.bio ?? '' }));
  }
}
