package com.example.learning.teaching.mq.config;

/** 仅教学案例使用的资源名，不进入服务业务代码。 */
public final class RabbitTeachingNames {
    public static final String SIMPLE = "learning.practice.rabbit.task.queue";
    public static final String DIRECT = "learning.practice.rabbit.order.direct.exchange";
    public static final String CREATED = "learning.practice.rabbit.order.created.queue";
    public static final String CREATED_KEY = "order.created";
    public static final String TOPIC = "learning.practice.rabbit.order.topic.exchange";
    public static final String TOPIC_STAR = "learning.practice.rabbit.order.star.queue";
    public static final String TOPIC_HASH = "learning.practice.rabbit.order.hash.queue";
    public static final String FANOUT = "learning.practice.rabbit.register.fanout.exchange";
    public static final String EMAIL = "learning.practice.rabbit.register.email.queue";
    public static final String SMS = "learning.practice.rabbit.register.sms.queue";
    public static final String AUDIT = "learning.practice.rabbit.register.audit.queue";
    public static final String PROCESS = "learning.practice.rabbit.order.process.queue";
    public static final String PROCESS_KEY = "order.process";
    public static final String DLX = "learning.practice.rabbit.order.dlx";
    public static final String DLQ = "learning.practice.rabbit.order.dlq";
    public static final String DEAD_KEY = "order.dead";
    /**
     * 仅保存练习资源常量，禁止实例化；learning.practice 前缀使练习队列与正式订单队列隔离。
     */
    private RabbitTeachingNames() {
    }
}
