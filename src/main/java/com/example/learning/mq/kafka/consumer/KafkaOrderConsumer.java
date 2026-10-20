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

    public KafkaOrderConsumer(OrderEventHandler handler) {
        this.handler = handler;
    }

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
