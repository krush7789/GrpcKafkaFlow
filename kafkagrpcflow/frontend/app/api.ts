// The browser only knows the gateway. It has no idea gRPC or Kafka exist.
const GATEWAY = process.env.NEXT_PUBLIC_GATEWAY_URL ?? 'http://localhost:3001';

export class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
  }
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = localStorage.getItem('token');
  const res = await fetch(GATEWAY + path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(init.headers as Record<string, string>),
    },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new ApiError(res.status, body.message ?? res.statusText);
  return body as T;
}
