package com.hvmello.core.concurrency.strategy1;

import com.hvmello.core.concurrency.Order;
import com.hvmello.core.concurrency.OrderProcessingStrategy;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ThreadPoolStrategy implements OrderProcessingStrategy {

    private final int poolSize;

    public ThreadPoolStrategy(int poolSize) {
        this.poolSize = poolSize;
    }

    @Override
    public void process(List<Order> orders) {
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        try {
            for (Order order : orders) {
                executor.submit(() -> processOrder(order));
            }
        } finally {
            executor.shutdown();
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void processOrder(Order order) {
        System.out.printf("[%s] Processing order %s - %s x%d%n",
                Thread.currentThread().getName(), order.id(), order.product(), order.quantity());
    }

    @Override
    public String name() {
        return "ThreadPool";
    }
}
