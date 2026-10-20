import { Body, Controller, Headers, HttpCode, Inject, OnModuleInit, Post } from '@nestjs/common';
import { ClientGrpc } from '@nestjs/microservices';
import { grpcCall } from './grpc-call';
import { AUTH, AuthService } from './grpc-clients';

@Controller('auth')
export class AuthController implements OnModuleInit {
  private auth: AuthService;

  constructor(@Inject(AUTH) private readonly client: ClientGrpc) {}

  onModuleInit() {
    this.auth = this.client.getService<AuthService>('AuthService');
  }

  // POST /auth/signup  { name, email, password }  ->  gRPC AuthService.SignUp
  // Optional header "Idempotency-Key: <uuid>" makes retries safe (same key -> same result).
  @Post('signup')
  signUp(
    @Body() body: { name?: string; email?: string; password?: string },
    @Headers('idempotency-key') idempotencyKey?: string,
  ) {
    return grpcCall(this.auth.signUp({
      name: body?.name ?? '',
      email: body?.email ?? '',
      password: body?.password ?? '',
      idempotencyKey: idempotencyKey ?? '',
    }));
  }

  // POST /auth/login  { email, password }  ->  gRPC AuthService.Login
  @Post('login')
  @HttpCode(200)
  login(@Body() body: { email?: string; password?: string }) {
    return grpcCall(this.auth.login({ email: body?.email ?? '', password: body?.password ?? '' }));
  }
}
