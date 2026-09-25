package com.example.learning.domain.order;

import com.example.learning.domain.product.Product;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OrderServiceTest {
    private final OrderService orderService = new OrderService(List.of(
            new Product("P-1001", "Notebook", new BigDecimal("19.90"))));

    @Test
    void calculatesOrderTotalFromThePriceAtOrderTime() {
        var order = orderService.createOrder("O-1001", "U-1001", "P-1001", 2);

        assertEquals(new BigDecimal("39.80"), order.getTotalAmount());
        assertEquals(new BigDecimal("19.90"), order.getUnitPriceAtOrder());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectsZeroOrNegativeQuantity(int quantity) {
        assertThrows(IllegalArgumentException.class,
                () -> orderService.createOrder("O-1002", "U-1001", "P-1001", quantity));
    }

    @Test
    void rejectsProductsThatAreNotInTheInMemoryCatalog() {
        assertThrows(IllegalArgumentException.class,
                () -> orderService.createOrder("O-1003", "U-1001", "missing", 1));
    }
}
