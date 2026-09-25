package com.example.learning.application;

import com.example.learning.domain.order.OrderService;
import com.example.learning.domain.product.Product;

import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Logger;

public final class LearningApplication {
    private static final Logger LOGGER = Logger.getLogger(LearningApplication.class.getName());

    private LearningApplication() {
    }

    public static void main(String[] args) {
        var product = new Product("P-1001", "Notebook", new BigDecimal("19.90"));
        var orderService = new OrderService(List.of(product));

        var order = orderService.createOrder("O-1001", "U-1001", "P-1001", 2);
        LOGGER.info(() -> "Created order " + order.getOrderId()
                + " with total " + order.getTotalAmount());
    }
}
