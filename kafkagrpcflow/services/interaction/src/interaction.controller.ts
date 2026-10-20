import { Controller, Inject, OnModuleInit } from '@nestjs/common';
import { ClientGrpc, GrpcMethod, RpcException } from '@nestjs/microservices';
import { status } from '@grpc/grpc-js';
import { randomUUID } from 'crypto';
import { firstValueFrom, Observable } from 'rxjs';
import { Db } from './db';

interface ProfileService {
  getProfile(req: { userId: string }): Observable<{ userId: string; name: string }>;
}

@Controller()
export class InteractionController implements OnModuleInit {
  private profiles: ProfileService;

  constructor(
    private readonly db: Db,
    @Inject('PROFILE_PACKAGE') private readonly profileClient: ClientGrpc,
  ) {}

  onModuleInit() {
    this.profiles = this.profileClient.getService<ProfileService>('ProfileService');
  }

  @GrpcMethod('InteractionService', 'Follow')
  async follow({ followerId, followeeId }: { followerId: string; followeeId: string }) {
    if (!followerId || !followeeId) {
      throw new RpcException({ code: status.INVALID_ARGUMENT, message: 'followerId and followeeId are required' });
    }
    if (followerId === followeeId) {
      throw new RpcException({ code: status.INVALID_ARGUMENT, message: 'you cannot follow yourself' });
    }

    // SYNC check over gRPC: we need the answer NOW to decide whether to accept the request.
    // This is exactly the situation where gRPC (request/response) beats Kafka (fire-and-forget).
    try {
      await firstValueFrom(this.profiles.getProfile({ userId: followeeId }));
    } catch (err: any) {
      if (err?.code === status.NOT_FOUND) {
        throw new RpcException({ code: status.NOT_FOUND, message: 'user to follow does not exist' });
      }
      throw new RpcException({ code: status.UNAVAILABLE, message: 'profile-service unavailable' });
    }

    const client = await this.db.pool.connect();
    try {
      await client.query('BEGIN');
      const inserted = await client.query(
        'INSERT INTO follows (follower_id, followee_id) VALUES ($1, $2) ON CONFLICT DO NOTHING',
        [followerId, followeeId],
      );
      if (inserted.rowCount === 0) {
        await client.query('ROLLBACK');
        return { success: true, message: 'already following' };
      }
      // ASYNC notification via Kafka: profile-service will bump the counters when it gets this.
      await client.query('INSERT INTO outbox (id, topic, message_key, payload) VALUES ($1, $2, $3, $4)', [
        randomUUID(), 'user.followed', followeeId, { followerId, followeeId },
      ]);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }

    console.log(`[interaction] Follow ok: ${followerId} -> ${followeeId}, user.followed saved to outbox`);
    return { success: true, message: 'followed' };
  }

  @GrpcMethod('InteractionService', 'GetFollowing')
  async getFollowing({ userId }: { userId: string }) {
    const { rows } = await this.db.pool.query('SELECT followee_id FROM follows WHERE follower_id = $1', [userId]);
    return { followeeIds: rows.map((r) => r.followee_id) };
  }
}
