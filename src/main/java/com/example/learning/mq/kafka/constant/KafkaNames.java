package com.example.learning.mq.kafka.constant;

/** Group 身份是消费进度的一部分；随意改组名会使新组按 auto-offset-reset 重新定位。 */
public final class KafkaNames {
    public static final String SIMPLE = "demo-topic";
    public static final String ORDER = "order-event-topic";
    public static final String ORDER_GROUP = "order-service-group";
    public static final String ANALYTICS_GROUP = "analytics-service-group";
    public static final String DLT_GROUP = "demo-dlt-observer-group";
    public static final String MESSAGE_ID_HEADER = "demo-message-id";
    public static final String BUSINESS_ID_HEADER = "demo-business-id";

    private KafkaNames() {
    }
}
