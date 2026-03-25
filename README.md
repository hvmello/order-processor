# order-processor

Production-grade distributed order processing system built with Java 21, Spring Boot, Kafka, and AWS. Demonstrates event-driven architecture, Saga pattern, resilience with Resilience4j, observability with Prometheus/Grafana, and cloud-native deployment.

---

## Why this project exists

Most backend systems work fine until something fails. A payment service goes down. A message gets duplicated. A downstream API times out at peak load.

This project was built to answer one question: **how do you process orders reliably when any downstream service can fail?**

Every architectural decision here was made to solve a real distributed systems problem — not to use popular technologies for the sake of it.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          API Gateway                            │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Order Service                              │
│          Spring Boot · Java 21 · PostgreSQL                     │
│          Outbox Pattern · Idempotency Keys                      │
└──────────┬──────────────────────────────────────────────────────┘
           │ Kafka (outbox events)
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Kafka Broker                               │
│          Topics: orders, payments, inventory, notifications     │
│          Dead Letter Queue per topic                            │
└──────┬───────────────┬───────────────┬───────────────────────────┘
       │               │               │
       ▼               ▼               ▼
┌──────────┐   ┌──────────────┐   ┌──────────────┐
│ Payment  │   │  Inventory   │   │Notification  │
│ Service  │   │   Service    │   │   Service    │
│  Lambda  │   │  Spring Boot │   │  Lambda+SNS  │
└──────────┘   └──────────────┘   └──────────────┘
       │               │
       └───────┬───────┘
               │ Saga orchestration
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Step Functions                               │
│         Orchestrates multi-step fulfillment workflow            │
└─────────────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Observability Stack                          │
│         Prometheus · Grafana · Micrometer · CloudWatch          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Modules

| Module | What it demonstrates |
|---|---|
| `core` | Java 21 concurrency (Virtual Threads, CompletableFuture), Stream API, SOLID principles, Design Patterns |
| `kafka-integration` | Event-driven architecture, at-least-once delivery, consumer groups, Dead Letter Queue |
| `aws-integration` | Lambda (Java), SQS fanout, SNS, Step Functions orchestration, IAM roles |
| `observability` | Micrometer metrics, Prometheus scraping, Grafana dashboards, p99 latency tracking |
| `resilience` | Resilience4j circuit breaker, retry with exponential backoff, bulkhead, outbox pattern |

---

## Key Technical Decisions

**Kafka over SQS for the messaging backbone**
Chose Kafka to support consumer group-based parallel processing and event replay capability. SQS would have been simpler to operate, but Kafka's offset management allows reprocessing failed events without data loss — critical for order consistency.

**Outbox pattern for dual-write safety**
Writing to both the database and Kafka in a single operation is a classic distributed systems trap. The outbox pattern solves this: the Order Service writes events to an outbox table in the same database transaction, and a separate poller publishes them to Kafka. The database commit is the source of truth — no event is ever lost, even if Kafka is temporarily unavailable.

**Saga over two-phase commit for distributed transactions**
Two-phase commit requires all participants to be available simultaneously — a distributed systems anti-pattern. Saga trades strong consistency for availability. Each step has a compensating transaction: if payment succeeds but inventory reservation fails, the payment is explicitly reversed. The trade-off accepted: compensating transactions must be designed upfront and tested carefully.

**Resilience4j circuit breaker on all external calls**
Every call to an external service is wrapped with a circuit breaker. When a service degrades, the circuit opens and fails fast — preventing cascading failures. Combined with exponential backoff retry and a bulkhead to isolate thread pools, the system degrades gracefully instead of collapsing.

**Virtual Threads for I/O-bound processing**
Java 21 Virtual Threads replace the traditional thread-per-request model for I/O-bound operations. Under load, this eliminates thread pool exhaustion without the complexity of reactive programming. Trade-off: not suitable for CPU-bound tasks, where platform threads still perform better.

---

## What I would do differently

**Extract the Saga orchestrator earlier.**
I coupled it too tightly to the Order Service in the first iteration, which made unit testing significantly harder. The orchestrator should be a first-class service from the start — isolated, independently deployable, and independently testable.

**Use cursor-based pagination from the beginning.**
I started with offset-based pagination for the order listing API. At high volume, offset pagination degrades badly because the database must scan and discard rows. Cursor-based pagination is the right default for any API that will grow.

---

## Running locally

Requirements: Docker, Java 21, Maven 3.9+

```bash
# Start infrastructure (Kafka, PostgreSQL, Prometheus, Grafana)
docker compose up -d

# Build all modules
mvn clean install

# Run Order Service
mvn spring-boot:run -pl core

# Access Grafana dashboard
open http://localhost:3000
```

---

## Observability

Grafana dashboards available at `http://localhost:3000` after running `docker compose up`.

Metrics tracked:
- Order processing throughput (orders/sec)
- End-to-end latency (p50, p95, p99)
- Kafka consumer lag per topic
- Circuit breaker state (closed / open / half-open)
- Dead letter queue depth

---

## Load testing

```bash
# Run load test with k6 (requires k6 installed)
k6 run load-tests/order-processor.js

# Results: throughput, p99 latency, error rate under load
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3 |
| Messaging | Apache Kafka |
| Database | PostgreSQL |
| Cloud | AWS (Lambda, SQS, SNS, Step Functions, RDS) |
| Resilience | Resilience4j |
| Observability | Prometheus, Grafana, Micrometer |
| Testing | JUnit 5, TestContainers, WireMock |
| Build | Maven |
| Containerization | Docker, Docker Compose |

---

## Topics

`java` `spring-boot` `kafka` `aws` `microservices` `distributed-systems` `event-driven` `saga-pattern` `resilience4j` `prometheus` `grafana` `postgresql` `docker` `testcontainers` `system-design`
