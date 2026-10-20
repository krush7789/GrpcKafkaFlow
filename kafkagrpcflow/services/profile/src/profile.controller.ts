import { Controller } from '@nestjs/common';
import { GrpcMethod, RpcException } from '@nestjs/microservices';
import { status } from '@grpc/grpc-js';
import { Db } from './db';

const COLUMNS = `user_id AS "userId", name, email, bio,
  followers_count AS "followersCount", following_count AS "followingCount"`;

@Controller()
export class ProfileController {
  constructor(private readonly db: Db) {}

  @GrpcMethod('ProfileService', 'GetProfile')
  async getProfile({ userId }: { userId: string }) {
    const { rows } = await this.db.pool.query(`SELECT ${COLUMNS} FROM profiles WHERE user_id = $1`, [userId]);
    // Right after signup this can briefly be NOT_FOUND: the Kafka event hasn't been consumed yet.
    if (!rows[0]) throw new RpcException({ code: status.NOT_FOUND, message: 'profile not found' });
    return rows[0];
  }

  @GrpcMethod('ProfileService', 'UpdateProfile')
  async updateProfile({ userId, bio }: { userId: string; bio: string }) {
    if ((bio ?? '').length > 280) {
      throw new RpcException({ code: status.INVALID_ARGUMENT, message: 'bio must be 280 characters or less' });
    }
    const { rows } = await this.db.pool.query(
      `UPDATE profiles SET bio = $2 WHERE user_id = $1 RETURNING ${COLUMNS}`,
      [userId, bio ?? ''],
    );
    if (!rows[0]) throw new RpcException({ code: status.NOT_FOUND, message: 'profile not found' });
    return rows[0];
  }

  @GrpcMethod('ProfileService', 'ListProfiles')
  async listProfiles() {
    // ponytail: no pagination, add page_token/page_size to the proto when there are many users
    const { rows } = await this.db.pool.query(`SELECT ${COLUMNS} FROM profiles ORDER BY created_at`);
    return { profiles: rows };
  }
}
