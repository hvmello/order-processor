package com.hvmello.orderprocessor.concurrency;

public record Order(String id, String customerId, double amount) {
}
