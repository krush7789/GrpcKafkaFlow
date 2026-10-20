import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { join } from 'path';
import { AppModule } from './app.module';

async function bootstrap() {
  const url = process.env.GRPC_URL ?? '0.0.0.0:50053';
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.GRPC,
    options: {
      package: 'interaction',
      protoPath: join(__dirname, '../../../proto/interaction.proto'),
      url,
      loader: { defaults: true },
      // Backpressure: max 100 in-flight calls per client connection. Extra calls wait on the
      // CLIENT side (and hit the caller's deadline) instead of piling up in this process's memory.
      channelOptions: { 'grpc.max_concurrent_streams': 100 },
    },
  });
  await app.listen();
  console.log(`[interaction] gRPC server listening on ${url}`);
}
bootstrap();
