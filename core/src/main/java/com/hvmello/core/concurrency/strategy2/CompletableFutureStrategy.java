package com.hvmello.core.concurrency.strategy2;

import com.hvmello.core.concurrency.Order;
import com.hvmello.core.concurrency.OrderProcessingStrategy;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CompletableFutureStrategy implements OrderProcessingStrategy {

    @Override
    public void process(List<Order> orders) {
        List<CompletableFuture<Void>> futures = orders.stream()
                .map(order -> CompletableFuture.runAsync(() -> processOrder(order)))
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void processOrder(Order order) {
        System.out.printf("[%s] Processing order %s - %s x%d%n",
                Thread.currentThread().getName(), order.id(), order.product(), order.quantity());
    }

    @Override
    public String name() {
        return "CompletableFuture";
    }
}
