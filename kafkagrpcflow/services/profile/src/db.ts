import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';

@Injectable()
export class Db implements OnModuleInit, OnModuleDestroy {
  readonly pool = new Pool({
    connectionString: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5434/profile_db',
  });

  async onModuleInit() {
    // ponytail: schema created on boot, use real migrations once the schema starts changing
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS profiles (
        user_id         UUID PRIMARY KEY,
        name            TEXT NOT NULL,
        email           TEXT NOT NULL,
        bio             TEXT NOT NULL DEFAULT '',
        followers_count INT  NOT NULL DEFAULT 0,
        following_count INT  NOT NULL DEFAULT 0,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      -- Idempotency: every Kafka event we've already applied. Duplicates are skipped.
      CREATE TABLE IF NOT EXISTS processed_events (
        event_id     UUID PRIMARY KEY,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);
  }

  onModuleDestroy() {
    return this.pool.end();
  }
}
