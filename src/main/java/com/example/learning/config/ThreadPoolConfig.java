package com.example.learning.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

// 1. 【核心规范】必须自定义业务专属线程池，绝对不能用 JDK 默认的 ForkJoinPool
@Configuration
public class ThreadPoolConfig {
    @Bean("profileExecutor")
    public Executor profileExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("Profile-Async-");
        // 拒绝策略：如果队列满了，由调用者（主线程）自己去执行，防止任务丢失
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
