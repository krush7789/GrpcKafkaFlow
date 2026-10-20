import { HttpException } from '@nestjs/common';
import { status } from '@grpc/grpc-js';
import { firstValueFrom, Observable, timeout, TimeoutError } from 'rxjs';

// gRPC has its own status codes. The browser understands HTTP ones. Translate.
const GRPC_TO_HTTP: Record<number, number> = {
  [status.INVALID_ARGUMENT]: 400,
  [status.UNAUTHENTICATED]: 401,
  [status.PERMISSION_DENIED]: 403,
  [status.NOT_FOUND]: 404,
  [status.ALREADY_EXISTS]: 409,
  [status.RESOURCE_EXHAUSTED]: 429,
  [status.UNAVAILABLE]: 503,
  [status.DEADLINE_EXCEEDED]: 504,
};

// Turns the gRPC Observable into a Promise, with a deadline, and maps errors to HTTP.
export async function grpcCall<T>(call: Observable<T>): Promise<T> {
  try {
    return await firstValueFrom(call.pipe(timeout(5000)));
  } catch (err: any) {
    if (err instanceof TimeoutError) throw new HttpException('upstream service timed out', 504);
    // Don't leak raw connection errors (hosts, ports) to the browser.
    if (err?.code === status.UNAVAILABLE) throw new HttpException('service unavailable', 503);
    throw new HttpException(err?.details || 'internal error', GRPC_TO_HTTP[err?.code] ?? 500);
  }
}
