package com.hvmello.orderprocessor.concurrency.strategy1;

import com.hvmello.orderprocessor.concurrency.Order;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolStrategy {
    private static final int THREAD_POOL_SIZE = 4;
    private final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    //thread control
    private final AtomicInteger processedCount = new AtomicInteger(0);

    public void processOrders(List<Order> orders) throws InterruptedException {
        System.out.println(STR."Processing \{orders.size()} orders with \{THREAD_POOL_SIZE} threads");

        for(Order order : orders) {
            executorService.submit(() -> processOrder(order));
        }

        //Awaits everything to finish
        //"The thread pool threads keep running waiting for new tasks. The JVM won't exit because these are non-daemon threads. Your application hangs."
        executorService.shutdown();
        boolean finished = executorService.awaitTermination(30, TimeUnit.SECONDS);

        if(!finished) {
            System.err.println("Failed to finish processing orders");
            executorService.shutdownNow();
        }

        System.out.println(STR."Total processed: \{processedCount.get()}");
    }

    private void processOrder(Order order) {
        try {
            // Emulating job (I/O, DB call)
            Thread.sleep(100);
            int count = processedCount.incrementAndGet();
            System.out.printf("[Thread: %s] Processed order %s | Total so far: %d%n",
                    Thread.currentThread().getName(), order.id(), count);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); //"When you catch InterruptedException, the interrupt flag is cleared.
            // If you don't restore it, calling code that checks the flag won't know the thread was interrupted.
            // It's a contract — you either re-interrupt or rethrow."
            System.err.println("Thread interrupted while processing order: " + order.id());
        }
    }


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

}
