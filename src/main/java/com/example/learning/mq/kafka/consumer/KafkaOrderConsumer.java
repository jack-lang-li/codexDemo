package com.example.learning.mq.kafka.consumer;

import com.example.learning.application.order.OrderEventHandler;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.kafka.constant.KafkaNames;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** 订单事件流订阅：业务成功后提交 offset，失败抛给 DefaultErrorHandler，不能 finally ACK。 */
@Component
@ConditionalOnProperty(name = {"app.mq.kafka.enabled", "app.mq.order-listeners-enabled"}, havingValue = "true")
public class KafkaOrderConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaOrderConsumer.class);
    private final OrderEventHandler handler;

    /**
     * 注入与 RabbitMQ 复用的订单处理函数，通过不同订阅者 scope 让两条业务链独立完成幂等。
     */
    public KafkaOrderConsumer(OrderEventHandler handler) {
        this.handler = handler;
    }

    /**
     * 由真实 Kafka 订单消费组拉取记录后触发，先完成订单业务和 Redis 幂等，再提交 offset。
     * 三个消费线程按分区分工；异常向容器传播以触发有限重试/DLT，不在 finally 提交失败记录。
     * @param record 实际 Broker 记录，含消息值、key、分区和 offset
     * @param acknowledgment 当前记录的提交句柄；消费线程中调用以配合 MANUAL_IMMEDIATE
     */
    @KafkaListener(topics = KafkaNames.ORDER, groupId = KafkaNames.ORDER_GROUP, concurrency = "3")
    public void onOrderCreated(ConsumerRecord<String, MessageEvent> record, Acknowledgment acknowledgment) {
        long start = System.nanoTime();
        MessageEvent event = record.value();
        try {
            String result = handler.onOrderCreated("kafka:order-event-processing", event);
            acknowledgment.acknowledge();
            LOGGER.info("[MQ-CONSUMER] middleware=Kafka group={} topic={} partition={} offset={} key={} messageId={} businessId={} result={} durationMs={}",
                    KafkaNames.ORDER_GROUP, record.topic(), record.partition(), record.offset(), record.key(),
                    event.messageId(), event.businessId(), result, (System.nanoTime() - start) / 1_000_000);
        } catch (RuntimeException exception) {
            LOGGER.error("[MQ-CONSUMER] middleware=Kafka topic={} partition={} offset={} key={} messageId={} businessId={} result=FAILED durationMs={}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    event == null ? null : event.messageId(), event == null ? null : event.businessId(),
                    (System.nanoTime() - start) / 1_000_000, exception);
            throw exception;
        }
    }
}
