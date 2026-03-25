package com.hvmello.core.concurrency.strategy3;

import com.hvmello.core.concurrency.Order;
import com.hvmello.core.concurrency.OrderProcessingStrategy;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VirtualThreadStrategy implements OrderProcessingStrategy {

    @Override
    public void process(List<Order> orders) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Order order : orders) {
                executor.submit(() -> processOrder(order));
            }
        }
    }

    private void processOrder(Order order) {
        System.out.printf("[%s] Processing order %s - %s x%d%n",
                Thread.currentThread().getName(), order.id(), order.product(), order.quantity());
    }

    @Override
    public String name() {
        return "VirtualThread";
    }
}
