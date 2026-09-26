package com.example.learning.demo.seckill;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Demonstrates per-product distributed locking around a stock operation. */
@Service
public class SeckillOrderService {
    private final RedissonClient redissonClient;
    private final StockService stockService;

    public SeckillOrderService(RedissonClient redissonClient, StockService stockService) {
        this.redissonClient = Objects.requireNonNull(redissonClient, "redissonClient");
        this.stockService = Objects.requireNonNull(stockService, "stockService");
    }

    public void deductStock(String productId) {
        if (productId == null || productId.isBlank()) {
            throw new IllegalArgumentException("productId must not be blank");
        }

        RLock lock = redissonClient.getLock("lock:product:stock:" + productId);
        boolean acquired = false;
        try {
            acquired = lock.tryLock(3, 10, TimeUnit.SECONDS);
            if (!acquired) {
                throw new IllegalStateException("Too many requests; please retry later");
            }
            stockService.deductStock(productId);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while acquiring the stock lock", exception);
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
