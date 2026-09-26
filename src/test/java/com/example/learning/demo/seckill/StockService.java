package com.example.learning.demo.seckill;

/** Port for the stock update protected by the distributed lock example. */
@FunctionalInterface
public interface StockService {
    void deductStock(String productId);
}
