import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';

@Injectable()
export class Db implements OnModuleInit, OnModuleDestroy {
  readonly pool = new Pool({
    connectionString: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5435/interaction_db',
  });

  async onModuleInit() {
    // ponytail: schema created on boot, use real migrations once the schema starts changing
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS follows (
        follower_id UUID NOT NULL,
        followee_id UUID NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (follower_id, followee_id)
      );
      CREATE TABLE IF NOT EXISTS outbox (
        id           UUID PRIMARY KEY,
        topic        TEXT NOT NULL,
        message_key  TEXT NOT NULL,
        payload      JSONB NOT NULL,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        published_at TIMESTAMPTZ
      );
    `);
  }

  onModuleDestroy() {
    return this.pool.end();
  }
}
