import { Injectable, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { Kafka, logLevel } from 'kafkajs';
import { Db } from './db';

const TOPICS = ['user.created', 'user.followed'];

// Listens to events published by OTHER services and updates profile_db.
// Nobody calls this; it reacts. That's the async half of the system.
@Injectable()
export class EventsConsumer implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly kafka = new Kafka({
    clientId: 'profile-service',
    brokers: [process.env.KAFKA_BROKER ?? 'localhost:9092'],
    logLevel: logLevel.WARN,
  });
  // groupId: all profile-service instances share the work; each message goes to ONE instance of the group.
  private readonly consumer = this.kafka.consumer({ groupId: 'profile-service' });

  constructor(private readonly db: Db) {}

  onApplicationBootstrap() {
    // Not awaited on purpose: the gRPC server keeps answering even while Kafka is unreachable.
    this.start();
  }

  private async start() {
    for (;;) {
      try {
        const admin = this.kafka.admin();
        await admin.connect();
        const existing = await admin.listTopics();
        const missing = TOPICS.filter((topic) => !existing.includes(topic));
        await admin.createTopics({ topics: missing.map((topic) => ({ topic, numPartitions: 3 })) });
        await admin.disconnect();

        await this.consumer.connect();
        await this.consumer.subscribe({ topics: TOPICS, fromBeginning: true });
        await this.consumer.run({
          // Offset is committed only after this function succeeds. If it throws, Kafka redelivers.
          eachMessage: async ({ topic, partition, message }) => {
            const event = JSON.parse(message.value!.toString());
            console.log(`[profile] kafka <- ${topic} p${partition} offset=${message.offset} eventId=${event.eventId}`);
            await this.handle(topic, event);
          },
        });
        console.log(`[profile] consuming ${TOPICS.join(', ')} as group "profile-service"`);
        return;
      } catch (err) {
        console.error('[profile] kafka not ready, retrying in 3s:', (err as Error).message);
        await this.consumer.disconnect().catch(() => {});
        await new Promise((resolve) => setTimeout(resolve, 3000));
      }
    }
  }

  private async handle(topic: string, event: any) {
    const client = await this.db.pool.connect();
    try {
      await client.query('BEGIN');
      // At-least-once delivery means the same event can arrive twice. Remember it, skip if seen.
      const seen = await client.query(
        'INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING',
        [event.eventId],
      );
      if (seen.rowCount === 0) {
        await client.query('ROLLBACK');
        console.log(`[profile] duplicate event ${event.eventId} skipped`);
        return;
      }

      if (topic === 'user.created') {
        await client.query(
          'INSERT INTO profiles (user_id, name, email) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO NOTHING',
          [event.userId, event.name, event.email],
        );
      } else if (topic === 'user.followed') {
        await client.query('UPDATE profiles SET followers_count = followers_count + 1 WHERE user_id = $1', [event.followeeId]);
        await client.query('UPDATE profiles SET following_count = following_count + 1 WHERE user_id = $1', [event.followerId]);
      }

      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err; // kafkajs retries the message
    } finally {
      client.release();
    }
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }
}
