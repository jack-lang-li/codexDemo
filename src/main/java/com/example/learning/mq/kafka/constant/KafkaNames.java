package com.example.learning.mq.kafka.constant;

/** Group 身份是消费进度的一部分；随意改组名会使新组按 auto-offset-reset 重新定位。 */
public final class KafkaNames {
    public static final String ORDER = "order-event-topic";
    public static final String ORDER_GROUP = "order-service-group";
    public static final String MESSAGE_ID_HEADER = "message-id";
    public static final String BUSINESS_ID_HEADER = "business-id";

    /**
     * 常量容器不需要实例；统一 Topic、消费组及消息头名称，避免生产和消费配置拼写不一致。
     */
    private KafkaNames() {
    }
}
