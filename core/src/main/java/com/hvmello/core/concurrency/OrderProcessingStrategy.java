package com.hvmello.core.concurrency;

import java.util.List;

public interface OrderProcessingStrategy {
    void process(List<Order> orders);
    String name();
}
