package com.example.learning.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 项目统一服务入口。配置默认从 application.yml 加载，不再单独启动 MQ 应用。
 * 业务、领域、通用配置与 MQ 均纳入同一个 Spring 容器；教学示例仅在测试中显式导入。
 */
@SpringBootApplication(scanBasePackages = {
        "com.example.learning.application", "com.example.learning.domain",
        "com.example.learning.config", "com.example.learning.mq"})
public class LearningApplication {
    /**
     * 启动唯一的 Spring Boot 服务，装配订单业务及已启用的 MQ 组件。
     * args 是命令行参数，按 Boot 规则可覆盖 application.yml；学习案例无需另建启动类。
     */
    public static void main(String[] args) {
        SpringApplication.run(LearningApplication.class, args);
    }
}
