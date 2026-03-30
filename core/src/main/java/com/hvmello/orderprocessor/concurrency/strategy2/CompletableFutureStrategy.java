package com.hvmello.orderprocessor.concurrency.strategy2;

import com.hvmello.orderprocessor.concurrency.Order;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletableFutureStrategy {

    // Dedicated thread pool with a fixed limit of 4 threads.
    // Never use the default ForkJoinPool.commonPool() in production —
    // it is shared across the entire JVM, so heavy tasks from other parts
    // of the application can cause unexpected slowdowns here.
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    // AtomicInteger because multiple threads increment this counter simultaneously.
    // A plain int would cause a race condition — two threads could read the same
    // value and overwrite each other's increment.
    // AtomicInteger uses CAS (compare-and-swap) at the hardware level:
    // no lock, no lost increment.
    private final AtomicInteger processedCount = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        var orders = List.of(
                new Order("ORD-001", "CUST-A", 150.00),
                new Order("ORD-002", "CUST-B", 89.90),
                new Order("ORD-003", "CUST-C", 320.00),
                new Order("ORD-004", "CUST-D", 45.50),
                new Order("ORD-005", "CUST-E", 210.00),
                new Order("ORD-006", "CUST-F", 99.00),
                new Order("ORD-007", "CUST-G", 175.00),
                new Order("ORD-008", "CUST-H", 540.00)
        );

        new CompletableFutureStrategy().processOrders(orders);
    }

    public void processOrders(List<Order> orders) throws InterruptedException {

        // For each order, build an independent async pipeline.
        // Different orders run in parallel across the thread pool,
        // but the 3 steps within each order are guaranteed to execute
        // in sequence — each stage only starts after the previous one completes.
        List<CompletableFuture<Void>> futures = orders.stream()
                .map(order -> CompletableFuture

                        // Step 1: submit the first task to our dedicated executor.
                        // supplyAsync returns a value (String) used by the next stage.
                        // Passing executorService explicitly avoids the shared commonPool.
                        .supplyAsync(() -> processOrder(order), executorService)

                        // Step 2: transforms the result from Step 1.
                        // thenApply is used here because the lambda returns a plain value (String).
                        // Use thenCompose instead when the lambda returns another CompletableFuture
                        // — otherwise you'd get CompletableFuture<CompletableFuture<T>>.
                        .thenApply(result -> notifyCustomer(order, result))

                        // Step 3: consumes the result from Step 2, returns nothing.
                        // thenAccept is the correct choice for the last stage of a pipeline
                        // when you don't need to pass a value forward.
                        .thenAccept(notified -> auditLog(order, notified))

                        // Isolates failures per order. If any stage throws,
                        // this catches the exception and logs it without
                        // affecting the pipelines of other orders.
                        .exceptionally(ex -> {
                            System.err.println("Failed to process order: " + order.id());
                            return null;
                        }))
                .toList();

        // allOf() creates a new CompletableFuture that completes only when
        // every pipeline in the list completes.
        // join() blocks the calling thread until that happens.
        // This is different from awaitTermination — that waits for the thread pool
        // to shut down, not for the tasks themselves to finish.
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .join();

        System.out.println("Total processed: " + processedCount.get());

        // shutdown() stops accepting new tasks but lets queued ones finish.
        // awaitTermination() blocks until all threads complete or timeout is reached.
        // Both are needed: shutdown() signals intent, awaitTermination() enforces it.
        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.SECONDS);
    }

    private String processOrder(Order order) {
        try {
            Thread.sleep(100); // simulates real I/O: database call, external API
            int count = processedCount.incrementAndGet(); // incrementAndGet: increment first, then return
            String result = "PROCESSED:" + order.id();
            System.out.printf("[%s] Step 1 - Processed %s | Total: %d%n",
                    Thread.currentThread().getName(), order.id(), count);
            return result;
        } catch (InterruptedException e) {
            // Catching InterruptedException clears the interrupt flag automatically.
            // Re-interrupting restores it so the caller can detect the interruption.
            // Throwing RuntimeException propagates the failure to exceptionally().
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted: " + order.id(), e);
        }
    }

    private String notifyCustomer(Order order, String processResult) {
        System.out.printf("[%s] Step 2 - Notified customer %s%n",
                Thread.currentThread().getName(), order.customerId());
        return "NOTIFIED:" + order.customerId();
    }

    private void auditLog(Order order, String notification) {
        System.out.printf("[%s] Step 3 - Audit log for %s%n",
                Thread.currentThread().getName(), order.id());
    }
}