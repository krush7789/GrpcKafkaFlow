import { Controller } from '@nestjs/common';
import { GrpcMethod, RpcException } from '@nestjs/microservices';
import { status } from '@grpc/grpc-js';
import { randomBytes, randomUUID, scrypt, timingSafeEqual } from 'crypto';
import { promisify } from 'util';
import * as jwt from 'jsonwebtoken';
import { Db } from './db';

const scryptAsync = promisify(scrypt) as (password: string, salt: string, keylen: number) => Promise<Buffer>;
const JWT_SECRET = process.env.JWT_SECRET ?? 'dev-secret-change-me';

async function hashPassword(password: string) {
  const salt = randomBytes(16).toString('hex');
  const hash = await scryptAsync(password, salt, 64);
  return `${salt}:${hash.toString('hex')}`;
}

async function verifyPassword(password: string, stored: string) {
  const [salt, hash] = stored.split(':');
  const candidate = await scryptAsync(password, salt, 64);
  return timingSafeEqual(candidate, Buffer.from(hash, 'hex'));
}

const signToken = (userId: string, email: string) =>
  jwt.sign({ sub: userId, email }, JWT_SECRET, { expiresIn: '1h' });

// Each @GrpcMethod maps to one `rpc` line in proto/auth.proto.
@Controller()
export class AuthController {
  constructor(private readonly db: Db) {}

  @GrpcMethod('AuthService', 'SignUp')
  async signUp({ name, email, password, idempotencyKey }: {
    name: string; email: string; password: string; idempotencyKey: string;
  }) {
    if (!name?.trim() || !email?.includes('@') || (password ?? '').length < 6) {
      throw new RpcException({
        code: status.INVALID_ARGUMENT,
        message: 'name, a valid email and a password (min 6 chars) are required',
      });
    }
    if ((idempotencyKey ?? '').length > 100) {
      throw new RpcException({ code: status.INVALID_ARGUMENT, message: 'idempotency key too long' });
    }
    email = email.trim().toLowerCase();

    // A retry of a signup that already succeeded? Return the original result, don't fail with 409.
    const replayed = await this.replay(idempotencyKey, email, password);
    if (replayed) return replayed;

    const userId = randomUUID();
    const passwordHash = await hashPassword(password);

    const client = await this.db.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('INSERT INTO users (id, name, email, password_hash) VALUES ($1, $2, $3, $4)', [
        userId, name.trim(), email, passwordHash,
      ]);
      // SAME transaction: the user row and the "user.created" event commit together, or not at all.
      // The OutboxRelay publishes this row to Kafka a moment later.
      await client.query('INSERT INTO outbox (id, topic, message_key, payload) VALUES ($1, $2, $3, $4)', [
        randomUUID(), 'user.created', userId, { userId, name: name.trim(), email },
      ]);
      // Remember the key in the SAME transaction: "key saved" <=> "user created".
      if (idempotencyKey) {
        await client.query('INSERT INTO idempotency_keys (key, user_id) VALUES ($1, $2)', [idempotencyKey, userId]);
      }
      await client.query('COMMIT');
    } catch (err: any) {
      await client.query('ROLLBACK');
      if (err.code === '23505') {
        // Two identical retries raced and the other one won: hand back its result.
        const raced = await this.replay(idempotencyKey, email, password);
        if (raced) return raced;
        throw new RpcException({ code: status.ALREADY_EXISTS, message: 'email already registered' });
      }
      throw err;
    } finally {
      client.release();
    }

    console.log(`[auth] SignUp ok: ${email} (${userId}), user.created saved to outbox`);
    return { userId, token: signToken(userId, email) };
  }

  private async replay(idempotencyKey: string, email: string, password: string) {
    if (!idempotencyKey) return null;
    const { rows } = await this.db.pool.query(
      `SELECT u.id, u.email, u.password_hash FROM idempotency_keys k JOIN users u ON u.id = k.user_id WHERE k.key = $1`,
      [idempotencyKey],
    );
    if (!rows[0]) return null;
    // Same key must mean the same request, and a replay must be as safe as a login (password checked).
    if (rows[0].email !== email || !(await verifyPassword(password, rows[0].password_hash))) {
      throw new RpcException({ code: status.INVALID_ARGUMENT, message: 'idempotency key was already used for a different request' });
    }
    console.log(`[auth] SignUp replayed for idempotency key ${idempotencyKey}, no new user created`);
    return { userId: rows[0].id, token: signToken(rows[0].id, rows[0].email) };
  }

  @GrpcMethod('AuthService', 'Login')
  async login({ email, password }: { email: string; password: string }) {
    const { rows } = await this.db.pool.query('SELECT id, email, password_hash FROM users WHERE email = $1', [
      (email ?? '').trim().toLowerCase(),
    ]);
    if (!rows[0] || !(await verifyPassword(password ?? '', rows[0].password_hash))) {
      throw new RpcException({ code: status.UNAUTHENTICATED, message: 'invalid email or password' });
    }
    return { userId: rows[0].id, token: signToken(rows[0].id, rows[0].email) };
  }

  @GrpcMethod('AuthService', 'ValidateToken')
  validateToken({ token }: { token: string }) {
    try {
      const payload = jwt.verify(token, JWT_SECRET) as jwt.JwtPayload;
      return { valid: true, userId: payload.sub, email: payload.email };
    } catch {
      return { valid: false, userId: '', email: '' };
    }
  }
}
