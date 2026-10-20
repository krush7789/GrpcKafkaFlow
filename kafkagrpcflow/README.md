# KafkaGrpcFlow

A tiny social app to learn **how gRPC and Kafka fit together** in a microservice system.

Next.js → NestJS API Gateway → (gRPC) → Auth / Profile / Interaction services, each with its own Postgres. Events via Kafka using the Transactional Outbox pattern.

## Run

Requires Node 20+ and Docker Desktop (running).

```bash
npm install
npm run infra:up    # Kafka :9092, Kafka UI :8080, Postgres :5433/:5434/:5435
npm run dev         # auth :50051, profile :50052, interaction :50053, gateway :3001, web :3000
```

Open http://localhost:3000 · Kafka UI http://localhost:8080 · Stop infra and wipe data with `npm run infra:down`.

## Docs (start here)

1. [docs/overall.md](docs/overall.md): the whole flow, told through Riya's signup
2. [docs/proto.md](docs/proto.md): the gRPC contracts
3. [docs/gateway.md](docs/gateway.md)
4. [docs/auth-service.md](docs/auth-service.md)
5. [docs/profile-service.md](docs/profile-service.md)
6. [docs/interaction-service.md](docs/interaction-service.md)
7. [docs/grpc-vs-kafka.md](docs/grpc-vs-kafka.md): when to use which + interview Q&A
8. [docs/idempotency-and-backpressure.md](docs/idempotency-and-backpressure.md): duplicates, idempotency keys, rate limiting, overload
