import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { join } from 'path';
import { Db } from './db';
import { InteractionController } from './interaction.controller';
import { OutboxRelay } from './outbox-relay';

@Module({
  imports: [
    // interaction-service is a gRPC SERVER (for the gateway) and a gRPC CLIENT (of profile-service).
    ClientsModule.register([
      {
        name: 'PROFILE_PACKAGE',
        transport: Transport.GRPC,
        options: {
          package: 'profile',
          protoPath: join(__dirname, '../../../proto/profile.proto'),
          url: process.env.PROFILE_GRPC_URL ?? 'localhost:50052',
          loader: { defaults: true },
        },
      },
    ]),
  ],
  controllers: [InteractionController],
  providers: [Db, OutboxRelay],
})
export class AppModule {}
