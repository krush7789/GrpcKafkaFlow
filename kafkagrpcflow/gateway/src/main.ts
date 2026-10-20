import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  // The gateway is the ONLY thing the browser talks to. Outside: REST/JSON over HTTP/1.1.
  // Inside: gRPC (protobuf over HTTP/2) to the services.
  const app = await NestFactory.create(AppModule);
  app.enableCors({ origin: process.env.FRONTEND_URL ?? 'http://localhost:3000' });
  const port = Number(process.env.PORT ?? 3001);
  await app.listen(port);
  console.log(`[gateway] REST API listening on http://localhost:${port}`);
}
bootstrap();
