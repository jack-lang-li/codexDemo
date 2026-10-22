package com.example.learning.mq.rabbitmq.constant;

/** 拓扑名称集中管理，便于在管理台与代码之间对应；不同学习环境应使用独立 vhost。 */
public final class RabbitMqNames {
    public static final String DIRECT = "order.direct.exchange";
    public static final String CREATED = "order.created.queue";
    public static final String CREATED_KEY = "order.created";
    public static final String DLX = "order.process.dlx";
    public static final String DLQ = "order.process.dlq";
    public static final String DEAD_KEY = "order.process.dead";

    /**
     * 仅供静态引用拓扑名称，禁止实例化；生产者、声明配置和消费者使用同一组名称避免路由不一致。
     */
    private RabbitMqNames() {
    }
}
