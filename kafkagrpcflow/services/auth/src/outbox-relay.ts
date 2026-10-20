import { Injectable, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { Kafka, logLevel, Partitioners } from 'kafkajs';
import { Db } from './db';

// Polls the outbox table and publishes unpublished rows to Kafka.
// Kafka down? Rows just wait in the table and go out when it's back. Nothing is lost.
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly kafka = new Kafka({
    clientId: 'auth-service',
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
    if (!(await admin.listTopics()).includes('user.created')) {
      await admin.createTopics({ topics: [{ topic: 'user.created', numPartitions: 3 }] });
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
      // Connect lazily, so the service boots (and SignUp works) even while Kafka is down.
      if (!this.connected) await this.connect();
      const { rows } = await this.db.pool.query(
        'SELECT id, topic, message_key, payload, created_at FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT 100',
      );
      for (const row of rows) {
        await this.producer.send({
          topic: row.topic,
          // key = userId -> all events of one user land in the same partition -> stay in order
          messages: [{ key: row.message_key, value: JSON.stringify({ eventId: row.id, occurredAt: row.created_at, ...row.payload }) }],
        });
        // If we crash right here the row is sent again on restart (at-least-once). Consumers dedupe by eventId.
        await this.db.pool.query('UPDATE outbox SET published_at = now() WHERE id = $1', [row.id]);
        console.log(`[auth] outbox -> kafka: ${row.topic} key=${row.message_key} eventId=${row.id}`);
      }
    } catch (err) {
      console.error('[auth] outbox relay error, will retry:', (err as Error).message);
    } finally {
      this.busy = false;
    }
  }

  async onModuleDestroy() {
    clearInterval(this.timer);
    await this.producer.disconnect();
  }
}
