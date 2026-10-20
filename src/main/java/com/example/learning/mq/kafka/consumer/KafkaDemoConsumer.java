package com.example.learning.mq.kafka.consumer;

import com.example.learning.mq.common.dto.MessageDemoDTO;
import com.example.learning.mq.common.service.DemoBusinessService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static com.example.learning.mq.kafka.constant.KafkaNames.*;

/**
 * 同 Group 的分区在同一时刻由一个 Consumer 成员负责；不同 Group 独立维护提交位置。
 * order group 的 3 个消费线程展示同组竞争/分区分配，analytics group 同时收到完整事件流。
 * Offset 是分区内日志位置；提交的是下次消费位置，不会像 RabbitMQ ACK 那样删除消息。
 */
@Component
public class KafkaDemoConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDemoConsumer.class);
    private final DemoBusinessService business;

    public KafkaDemoConsumer(DemoBusinessService business) {
        this.business = business;
    }

    @KafkaListener(topics = SIMPLE, groupId = "${spring.kafka.consumer.group-id}")
    public void simple(ConsumerRecord<String, MessageDemoDTO> record, Acknowledgment acknowledgment,
            @Header(KafkaHeaders.GROUP_ID) String group) {
        consume(group, record, acknowledgment);
    }

    @KafkaListener(topics = ORDER, groupId = ORDER_GROUP, concurrency = "3")
    public void order(ConsumerRecord<String, MessageDemoDTO> record, Acknowledgment acknowledgment) {
        consume(ORDER_GROUP, record, acknowledgment);
    }

    @KafkaListener(topics = ORDER, groupId = ANALYTICS_GROUP)
    public void analytics(ConsumerRecord<String, MessageDemoDTO> record, Acknowledgment acknowledgment) {
        consume(ANALYTICS_GROUP, record, acknowledgment);
    }

    @KafkaListener(topics = {SIMPLE + ".DLT", ORDER + ".DLT"}, groupId = DLT_GROUP,
            containerFactory = "kafkaDltContainerFactory",
            autoStartup = "${demo.mq.dlt-listener-enabled:false}",
            properties = {"value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer"})
    public void deadLetter(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        // 使用 byte[] 观察 DLT，兼容原消息为非法 JSON 的情况，避免 DLT 自己再次反序列化失败。
        // 默认关闭；开启后提交的只是观察组 offset，不会删除 Kafka 的历史记录。
        LOGGER.warn("[MQ-CONSUMER] middleware=Kafka group={} topic={} partition={} offset={} key={} messageId={} businessId={} bytes={} headers={} result=DLT_OBSERVED durationMs=0",
                DLT_GROUP, record.topic(), record.partition(), record.offset(), record.key(),
                textHeader(record, MESSAGE_ID_HEADER), textHeader(record, BUSINESS_ID_HEADER),
                record.value() == null ? 0 : record.value().length, Arrays.toString(record.headers().toArray()));
        acknowledgment.acknowledge();
    }

    private void consume(String group, ConsumerRecord<String, MessageDemoDTO> record, Acknowledgment acknowledgment) {
        long start = System.nanoTime();
        MessageDemoDTO event = record.value();
        try {
            if (event == null) {
                throw new IllegalArgumentException("本学习协议不接收空消息/tombstone");
            }
            String result = business.process("kafka:" + group + ":" + record.topic(), event);
            // 业务成功+Redis DONE 后才在消费线程提交。MANUAL_IMMEDIATE 在消费线程立即提交。
            // 若在业务完成后、提交前宕机，消息仍可能重投，这就是业务幂等不可缺少的原因。
            acknowledgment.acknowledge();
            LOGGER.info("[MQ-CONSUMER] middleware=Kafka group={} topic={} partition={} offset={} key={} messageId={} businessId={} timestamp={} eventType={} message={} attempt={} result={} durationMs={}",
                    group, record.topic(), record.partition(), record.offset(), record.key(),
                    event.messageId(), event.businessId(), record.timestamp(), event.eventType(), event,
                    attempt(record), result,
                    (System.nanoTime() - start) / 1_000_000);
        } catch (RuntimeException exception) {
            // 不能 catch 后吞掉异常或在 finally ACK，否则容器将跳过失败消息，无法重试/DLT。
            LOGGER.error("[MQ-CONSUMER] middleware=Kafka group={} topic={} partition={} offset={} key={} messageId={} businessId={} result=FAILED durationMs={}",
                    group, record.topic(), record.partition(), record.offset(), record.key(),
                    event == null ? null : event.messageId(), event == null ? null : event.businessId(),
                    (System.nanoTime() - start) / 1_000_000, exception);
            throw exception;
        }
    }

    private int attempt(ConsumerRecord<?, ?> record) {
        var header = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        return header == null || header.value() == null || header.value().length != Integer.BYTES
                ? 1 : java.nio.ByteBuffer.wrap(header.value()).getInt();
    }

    private String textHeader(ConsumerRecord<?, ?> record, String name) {
        var header = record.headers().lastHeader(name);
        // 非本应用产生的损坏消息可能没有业务 ID，仍保留原字节和来源 offset 供排查。
        return header == null || header.value() == null ? "unknown" : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }
}
