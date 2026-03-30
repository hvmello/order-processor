package com.hvmello.orderprocessor.concurrency.strategy1;

import com.hvmello.orderprocessor.concurrency.Order;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolStrategy {

    // Fixed thread count avoids unbounded thread creation.
    // newCachedThreadPool() has no limit — under a traffic spike it creates
    // one thread per task, leading to OutOfMemoryError or excessive context switching.
    // newFixedThreadPool gives predictable, bounded resource usage.
    private static final int THREAD_POOL_SIZE = 4;
    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    // AtomicInteger because multiple threads increment this counter simultaneously.
    // A plain int would cause a race condition — two threads could read the same value,
    // both increment locally, and write back the same result, losing one increment.
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

        new ThreadPoolStrategy().processOrders(orders);
    }

    public void processOrders(List<Order> orders) throws InterruptedException {
        System.out.println("Processing " + orders.size() + " orders with " + THREAD_POOL_SIZE + " threads");

        // submit() places each task in the pool's internal BlockingQueue.
        // If a thread is available, it picks the task immediately.
        // If all 4 threads are busy, the task waits in the queue —
        // no new thread is created, no task is dropped.
        // This loop returns almost instantly — tasks run asynchronously.
        for (Order order : orders) {
            executorService.submit(() -> processOrder(order));
        }

        // shutdown() stops accepting new tasks but lets queued and running ones finish.
        // It returns immediately — it does NOT block.
        // Without shutdown(), the pool threads keep running waiting for new tasks.
        // The JVM won't exit because these are non-daemon threads — the app hangs.
        executorService.shutdown();

        // awaitTermination() is what actually blocks the calling thread
        // until all tasks complete or the timeout expires.
        // shutdown() signals intent, awaitTermination() enforces the wait.
        boolean finished = executorService.awaitTermination(30, TimeUnit.SECONDS);

        if (!finished) {
            // Timeout expired before all tasks finished.
            // shutdownNow() attempts to interrupt running threads by calling interrupt()
            // on each — but does NOT guarantee they stop. It depends on the task
            // checking the interrupt flag. It also returns tasks still in the queue
            // that were never executed.
            System.err.println("Failed to finish processing orders");
            executorService.shutdownNow();
        }

        System.out.println("Total processed: " + processedCount.get());
    }

    private void processOrder(Order order) {
        try {
            // Simulates real I/O: database call, external API, file read.
            // During sleep the thread is in TIMED_WAITING state —
            // it releases the CPU and does not consume processing time.
            Thread.sleep(100);

            // incrementAndGet: increments first, then returns the new value.
            // getAndIncrement would return the old value before incrementing.
            int count = processedCount.incrementAndGet();
            System.out.printf("[Thread: %s] Processed order %s | Total so far: %d%n",
                    Thread.currentThread().getName(), order.id(), count);
        } catch (InterruptedException e) {
            // Catching InterruptedException automatically clears the thread's interrupt flag.
            // Re-interrupting restores it so any calling code that checks
            // Thread.isInterrupted() — such as a shutdown mechanism — still sees the signal.
            // Swallowing the exception without re-interrupting breaks that contract.
            Thread.currentThread().interrupt();
            System.err.println("Thread interrupted while processing order: " + order.id());
        }
    }
}