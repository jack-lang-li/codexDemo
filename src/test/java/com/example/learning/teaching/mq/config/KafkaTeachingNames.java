package com.example.learning.teaching.mq.config;

/** 不同组独立订阅及基础任务 Topic，仅用于学习案例。 */
public final class KafkaTeachingNames {
    public static final String SIMPLE = "learning.practice.kafka.tasks";
    public static final String ORDER = "learning.practice.kafka.order.events";
    public static final String ORDER_GROUP = "learning-practice-order-group";
    public static final String ANALYTICS_GROUP = "learning-practice-analytics-group";
    public static final String SIMPLE_GROUP = "learning-practice-task-group";
    public static final String DLT_GROUP = "learning-practice-dlt-observer-group";
    /**
     * 仅保存练习 Topic 与消费组身份，禁止实例化；独立组确保测试进度不会覆盖正式业务进度。
     */
    private KafkaTeachingNames() {
    }
}
