import { CanActivate, ExecutionContext, HttpException, Injectable } from '@nestjs/common';

const LIMIT = Number(process.env.RATE_LIMIT ?? 50); // max requests per IP per window
const WINDOW_MS = Number(process.env.RATE_WINDOW_MS ?? 10_000);

// Load shedding at the front door: over the limit -> 429 right here, so the extra
// requests never reach the gRPC services or their databases.
// ponytail: fixed window, in memory, per gateway instance; use a shared Redis counter when running several gateways
@Injectable()
export class RateLimitGuard implements CanActivate {
  private hits = new Map<string, number>();
  private windowStart = Date.now();

  canActivate(ctx: ExecutionContext) {
    const now = Date.now();
    if (now - this.windowStart >= WINDOW_MS) {
      this.hits.clear();
      this.windowStart = now;
    }

    const req = ctx.switchToHttp().getRequest();
    const ip = req.ip ?? 'unknown';
    const count = (this.hits.get(ip) ?? 0) + 1;
    this.hits.set(ip, count);

    if (count > LIMIT) {
      const retryAfter = Math.ceil((this.windowStart + WINDOW_MS - now) / 1000);
      ctx.switchToHttp().getResponse().setHeader('Retry-After', String(retryAfter));
      throw new HttpException('too many requests, slow down', 429);
    }
    return true;
  }
}
