import { Injectable, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { Kafka, logLevel, Partitioners } from 'kafkajs';
import { Db } from './db';

// Same pattern as services/auth/src/outbox-relay.ts. Each service owns its copy (no shared code between services).
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly kafka = new Kafka({
    clientId: 'interaction-service',
    brokers: [process.env.KAFKA_BROKER ?? 'localhost:9092'],
    logLevel: logLevel.WARN,
  });
  private readonly producer = this.kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  private timer?: NodeJS.Timeout;
  private busy = false;
  private connected = false;

  constructor(private readonly db: Db) {}

  onApplicationBootstrap() {
    this.timer = setInterval(() => this.publishPending(), 1000);
  }

  private async connect() {
    const admin = this.kafka.admin();
    await admin.connect();
    if (!(await admin.listTopics()).includes('user.followed')) {
      await admin.createTopics({ topics: [{ topic: 'user.followed', numPartitions: 3 }] });
    }
    await admin.disconnect();
    await this.producer.connect();
    this.connected = true;
  }

  // ponytail: single relay instance; with several instances use SELECT ... FOR UPDATE SKIP LOCKED (or Debezium CDC)
  private async publishPending() {
    if (this.busy) return;
    this.busy = true;
    try {
      if (!this.connected) await this.connect();
      const { rows } = await this.db.pool.query(
        'SELECT id, topic, message_key, payload, created_at FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 100',
      );
      for (const row of rows) {
        await this.producer.send({
          topic: row.topic,
          messages: [{ key: row.message_key, value: JSON.stringify({ eventId: row.id, occurredAt: row.created_at, ...row.payload }) }],
        });
        await this.db.pool.query('UPDATE outbox SET published_at = now() WHERE id = $1', [row.id]);
        console.log(`[interaction] outbox -> kafka: ${row.topic} key=${row.message_key} eventId=${row.id}`);
      }
    } catch (err) {
      console.error('[interaction] outbox relay error, will retry:', (err as Error).message);
    } finally {
      this.busy = false;
    }
  }

  async onModuleDestroy() {
    clearInterval(this.timer);
    await this.producer.disconnect();
  }
}
