import { Module } from '@nestjs/common';
import { AuthController } from './auth.controller';
import { Db } from './db';
import { OutboxRelay } from './outbox-relay';

@Module({
  controllers: [AuthController],
  providers: [Db, OutboxRelay],
})
export class AppModule {}
