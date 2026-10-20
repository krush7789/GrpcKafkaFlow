import { CanActivate, ExecutionContext, Inject, Injectable, UnauthorizedException } from '@nestjs/common';
import { ClientGrpc } from '@nestjs/microservices';
import { grpcCall } from './grpc-call';
import { AUTH, AuthService } from './grpc-clients';

// Runs before protected routes. Asks auth-service (gRPC) "is this token valid? whose is it?"
@Injectable()
export class AuthGuard implements CanActivate {
  constructor(@Inject(AUTH) private readonly client: ClientGrpc) {}

  async canActivate(ctx: ExecutionContext) {
    const req = ctx.switchToHttp().getRequest();
    const token = req.headers.authorization?.replace(/^Bearer /, '');
    if (!token) throw new UnauthorizedException('missing token');

    const auth = this.client.getService<AuthService>('AuthService');
    const res = await grpcCall(auth.validateToken({ token }));
    if (!res.valid) throw new UnauthorizedException('invalid or expired token');

    req.userId = res.userId; // controllers read the caller's id from here, never from the body
    return true;
  }
}
