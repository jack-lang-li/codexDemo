package com.example.learning.mq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 独立的 MQ 学习入口。原来的纯 Java LearningApplication 仍可单独运行。
 * 扫描范围只包含 mq 包，避免把其他学习实验或未来业务自动注册进本次应用。
 * mq profile 隔离了预置但尚未配置的数据库、注册中心、网关等依赖。
 */
@SpringBootApplication
public class MqDemoApplication {
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(MqDemoApplication.class);
        application.setAdditionalProfiles("mq");
        application.run(args);
    }
}
