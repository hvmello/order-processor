# order-processor

Two runnable Java 21 implementations of the same job: process a batch of orders concurrently, with a
bounded thread pool, and shut down cleanly. The comments explain why each concurrency primitive was
chosen and what breaks without it.

> **Status: early stage.** Only the `core` module has code today. `api`, `messaging`, `observability`
> and `ai` are empty Maven modules reserved for later work.

## See it working

No dependencies needed, only JDK 21. From the repository root:

```bash
javac --release 21 -d out $(find core -name '*.java')
java -cp out com.hvmello.orderprocessor.concurrency.strategy1.ThreadPoolStrategy
```

Real output, 8 orders on 4 threads:

```text
Processing 8 orders with 4 threads
[Thread: pool-1-thread-3] Processed order ORD-003 | Total so far: 1
[Thread: pool-1-thread-2] Processed order ORD-002 | Total so far: 3
[Thread: pool-1-thread-4] Processed order ORD-004 | Total so far: 2
[Thread: pool-1-thread-1] Processed order ORD-001 | Total so far: 4
[Thread: pool-1-thread-1] Processed order ORD-008 | Total so far: 6
[Thread: pool-1-thread-3] Processed order ORD-005 | Total so far: 7
[Thread: pool-1-thread-2] Processed order ORD-006 | Total so far: 8
[Thread: pool-1-thread-4] Processed order ORD-007 | Total so far: 5
Total processed: 8
```

The completion order shuffles and the counter still reaches exactly 8: no increment is lost.

## The two strategies

| | `ThreadPoolStrategy` | `CompletableFutureStrategy` |
|---|---|---|
| Model | Submit one task per order | One async pipeline per order |
| Steps per order | Process | Process, notify customer, write audit log |
| Waits with | `shutdown()` + `awaitTermination()` | `allOf(...).join()` |
| Failure handling | Timeout falls back to `shutdownNow()` | `exceptionally()` isolates each order |

## Decisions worth reading in the code

- **Fixed pool instead of a cached pool**, so a traffic spike queues work instead of creating unbounded threads.
- **Dedicated executor instead of `commonPool()`**, so unrelated JVM work cannot starve the pipeline.
- **`AtomicInteger` for the shared counter**, because a plain `int` loses increments under contention.
- **Re-interrupting after `InterruptedException`**, so the shutdown signal still reaches the caller.

## Limitations

I/O is simulated with `Thread.sleep(100)`, orders are hard-coded, and there are no automated tests yet.
