package com.hvmello.core.concurrency.strategy1;

import com.hvmello.core.concurrency.Order;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class ThreadPoolStrategyTest {

    private final ThreadPoolStrategy strategy = new ThreadPoolStrategy(4);

    @Test
    void shouldProcessAllOrders() {
        var orders = List.of(
                new Order("1", "Laptop", 1),
                new Order("2", "Mouse", 2),
                new Order("3", "Keyboard", 1)
        );

        assertDoesNotThrow(() -> strategy.process(orders));
    }

    @Test
    void shouldHandleEmptyOrderList() {
        assertDoesNotThrow(() -> strategy.process(List.of()));
    }
}
