import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';

@Injectable()
export class Db implements OnModuleInit, OnModuleDestroy {
  readonly pool = new Pool({
    connectionString: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5433/auth_db',
  });

  async onModuleInit() {
    // ponytail: schema created on boot, use real migrations once the schema starts changing
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id            UUID PRIMARY KEY,
        name          TEXT NOT NULL,
        email         TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      -- Transactional outbox: events waiting to be published to Kafka.
      CREATE TABLE IF NOT EXISTS outbox (
        id           UUID PRIMARY KEY,
        topic        TEXT NOT NULL,
        message_key  TEXT NOT NULL,
        payload      JSONB NOT NULL,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        published_at TIMESTAMPTZ
      );
      -- Idempotency keys: "this SignUp request already succeeded and created this user".
      -- ponytail: kept forever, delete rows older than ~24h with a cron once the table grows
      CREATE TABLE IF NOT EXISTS idempotency_keys (
        key        TEXT PRIMARY KEY,
        user_id    UUID NOT NULL REFERENCES users(id),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
  }

  onModuleDestroy() {
    return this.pool.end();
  }
}
