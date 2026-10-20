package com.example.learning.mq.rabbitmq.constant;

/** 拓扑名称集中管理，便于在管理台与代码之间对应；不同学习环境应使用独立 vhost。 */
public final class RabbitMqNames {
    public static final String SIMPLE = "demo.task.queue";
    public static final String DIRECT = "order.direct.exchange";
    public static final String CREATED = "order.created.queue";
    public static final String CREATED_KEY = "order.created";
    public static final String TOPIC = "order.topic.exchange";
    public static final String TOPIC_STAR = "order.topic.star.queue";
    public static final String TOPIC_HASH = "order.topic.hash.queue";
    public static final String FANOUT = "user.register.fanout.exchange";
    public static final String EMAIL = "user.register.email.queue";
    public static final String SMS = "user.register.sms.queue";
    public static final String AUDIT = "user.register.audit.queue";
    public static final String PROCESS = "order.process.queue";
    public static final String PROCESS_KEY = "order.process";
    public static final String DLX = "order.process.dlx";
    public static final String DLQ = "order.process.dlq";
    public static final String DEAD_KEY = "order.process.dead";

    private RabbitMqNames() {
    }
}
