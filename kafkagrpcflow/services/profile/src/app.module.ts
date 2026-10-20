import { Module } from '@nestjs/common';
import { Db } from './db';
import { EventsConsumer } from './events.consumer';
import { ProfileController } from './profile.controller';

@Module({
  controllers: [ProfileController],
  providers: [Db, EventsConsumer],
})
export class AppModule {}
