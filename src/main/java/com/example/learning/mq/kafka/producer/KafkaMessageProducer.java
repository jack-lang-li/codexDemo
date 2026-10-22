package com.example.learning.mq.kafka.producer;

import com.example.learning.mq.common.dto.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka 使用业务 ID 作为 key，在分区数量和分区器不变时，同 key 通常进入同一分区。
 * 顺序只在分区内成立；扩分区可能改变 key 映射，多生产者并发也不等于业务因果顺序。
 * enable.idempotence 防止 Producer 协议重试重复写入，无法去重显式发送两次或消费者重投。
 */
@Component
@ConditionalOnProperty(name = "app.mq.kafka.enabled", havingValue = "true")
public class KafkaMessageProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducer.class);
    private final KafkaTemplate<Object, Object> template;

    /**
     * 注入读取现有 YAML 配置的真实 KafkaTemplate，统一复用 JSON 序列化和 Broker 确认设置。
     */
    public KafkaMessageProducer(KafkaTemplate<Object, Object> template) {
        this.template = template;
    }

    /**
     * 使用业务 ID 作为 key 异步发布 JSON 消息，Broker 回执返回后记录实际分区和 offset。
     * 调用者应等待或处理返回 Future 的异常；仅调用 send 而忽略结果，无法判断发送是否成功。
     * @param topic 目标 Topic 名称
     * @param event 本次事件，businessId 是分区 key，messageId 用于消费去重
     * @return Broker 确认成功后携带原事件的 Future；发送失败则异常完成，尚不代表消费者已处理
     */
    public CompletableFuture<MessageEvent> send(String topic, MessageEvent event) {
        LOGGER.info("[MQ-PRODUCER] middleware=Kafka topic={} key={} messageId={} businessId={} result=SENDING",
                topic, event.businessId(), event.messageId(), event.businessId());
        return template.send(topic, event.businessId(), event).whenComplete((result, exception) -> {
            if (exception != null) {
                LOGGER.error("[MQ-PRODUCER] middleware=Kafka topic={} key={} messageId={} businessId={} result=FAILED",
                        topic, event.businessId(), event.messageId(), event.businessId(), exception);
            } else {
                var metadata = result.getRecordMetadata();
                LOGGER.info("[MQ-PRODUCER] middleware=Kafka topic={} partition={} offset={} key={} messageId={} businessId={} result=BROKER_ACK",
                        metadata.topic(), metadata.partition(), metadata.offset(), event.businessId(),
                        event.messageId(), event.businessId());
            }
        }).thenApply(result -> event);
    }
}
