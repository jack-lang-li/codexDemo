package com.example.learning.lab.concurrency;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public final class RaceConditionExperiment {
    private static final int WORKER_COUNT = 2;
    private static final int INCREMENTS_PER_WORKER = 100_000;

    private RaceConditionExperiment() {
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var counter = new PlainCounter();
        var ready = new CountDownLatch(WORKER_COUNT);
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(WORKER_COUNT);

        try {
            // 返回 null 可让 lambda 匹配 Callable；工作线程的 InterruptedException
            // 会由 Future 保存，并在下面的 get() 中以 ExecutionException 报告。
            var first = executor.submit(() -> {
                increment(counter, ready, start);
                return null;
            });
            var second = executor.submit(() -> {
                increment(counter, ready, start);
                return null;
            });

            ready.await();
            start.countDown();
            first.get();
            second.get();

            int expected = WORKER_COUNT * INCREMENTS_PER_WORKER;
            System.out.println("Expected count: " + expected);
            System.out.println("Actual count:   " + counter.value);
        } finally {
            // 即使主线程在等待时被中断，也释放可能仍在等待 start 的工作线程。
            start.countDown();
            executor.shutdownNow();
        }
    }

    private static void increment(PlainCounter counter, CountDownLatch ready,
                                  CountDownLatch start) throws InterruptedException {
        ready.countDown();
        start.await();
        for (int i = 0; i < INCREMENTS_PER_WORKER; i++) {
            counter.increment();
        }
    }

    private static final class PlainCounter {
        private int value;

        private void increment() {
            // 学习重点：++ 是读取、加一、写回的复合操作，不保证原子性。
            value++;
        }
    }
}
