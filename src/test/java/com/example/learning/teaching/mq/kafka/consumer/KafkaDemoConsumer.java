package com.example.learning.teaching.mq.kafka.consumer;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.teaching.mq.common.service.DemoBusinessService;
import com.example.learning.teaching.mq.common.service.MqConsumptionProbe;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Arrays;

import static com.example.learning.mq.kafka.constant.KafkaNames.MESSAGE_ID_HEADER;
import static com.example.learning.mq.kafka.constant.KafkaNames.BUSINESS_ID_HEADER;
import static com.example.learning.teaching.mq.config.KafkaTeachingNames.*;

/**
 * 同 Group 的分区在同一时刻由一个 Consumer 成员负责；不同 Group 独立维护提交位置。
 * order group 的 3 个消费线程展示同组竞争/分区分配，analytics group 同时收到完整事件流。
 * Offset 是分区内日志位置；提交的是下次消费位置，不会像 RabbitMQ ACK 那样删除消息。
 */
@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "app.mq.kafka.enabled", havingValue = "true")
public class KafkaDemoConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDemoConsumer.class);
    private final DemoBusinessService business;
    private final MqConsumptionProbe probe;

    /**
     * 注入教学业务与被动结果记录器；消息始终由真实 Kafka 客户端拉取并交给监听器。
     */
    public KafkaDemoConsumer(DemoBusinessService business, MqConsumptionProbe probe) {
        this.business = business;
        this.probe = probe;
    }

    /**
     * 基础 Topic 的真实监听入口，从 Kafka 消息头获取实际组名，交给公共消费步骤处理后提交。
     * 日志会展示 Topic、分区、offset、key 和事件，便于对照生产者 Broker 回执。
     */
    @KafkaListener(topics = SIMPLE, groupId = SIMPLE_GROUP)
    public void simple(ConsumerRecord<String, MessageEvent> record, Acknowledgment acknowledgment,
            @Header(KafkaHeaders.GROUP_ID) String group) {
        consume(group, record, acknowledgment);
    }

    /**
     * 订单处理组的监听入口，三个消费线程按分区分工，展示同组内的竞争消费。
     * 同一分区同一时刻分配给组内一个成员，不会让三个线程各获得每条消息的副本。
     */
    @KafkaListener(topics = ORDER, groupId = ORDER_GROUP, concurrency = "3")
    public void order(ConsumerRecord<String, MessageEvent> record, Acknowledgment acknowledgment) {
        consume(ORDER_GROUP, record, acknowledgment);
    }

    /**
     * 统计组独立订阅同一个订单 Topic，使用自己的 offset 与幂等范围。
     * 展示一次订单事件发布后，订单处理与统计两个业务可以分别消费，而无需生产者发送两份消息。
     */
    @KafkaListener(topics = ORDER, groupId = ANALYTICS_GROUP)
    public void analytics(ConsumerRecord<String, MessageEvent> record, Acknowledgment acknowledgment) {
        consume(ANALYTICS_GROUP, record, acknowledgment);
    }

    /**
     * 可选 DLT 观察器，以 byte[] 接收原始数据，兼容正常事件失败和损坏 JSON 两种死信。
     * 默认关闭；开启后只记录并提交观察组 offset，不再次调用故障业务，也不删除 Kafka 历史记录。
     */
    @KafkaListener(topics = {SIMPLE + ".DLT", ORDER + ".DLT"}, groupId = DLT_GROUP,
            containerFactory = "kafkaDltContainerFactory",
            autoStartup = "${teaching.mq.dlt-listener-enabled:false}",
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

    /**
     * 真实 Kafka 消费的共同步骤：校验消息、按 group/topic 做 Redis 幂等、完成日志业务，再提交 offset。
     * 提交成功才记录可断言结果；失败必须抛给 DefaultErrorHandler，不能吞异常或在 finally 提交。
     */
    private void consume(String group, ConsumerRecord<String, MessageEvent> record, Acknowledgment acknowledgment) {
        long start = System.nanoTime();
        MessageEvent event = record.value();
        try {
            if (event == null) {
                throw new IllegalArgumentException("本学习协议不接收空消息/tombstone");
            }
            String result = business.process("kafka:" + group + ":" + record.topic(), event);
            // 业务成功+Redis DONE 后才在消费线程提交。MANUAL_IMMEDIATE 在消费线程立即提交。
            // 若在业务完成后、提交前宕机，消息仍可能重投，这就是业务幂等不可缺少的原因。
            acknowledgment.acknowledge();
            probe.received("kafka:" + group + ":" + record.topic(), event, result,
                    record.partition(), record.offset(), record.key());
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

    /**
     * 读取容器附加的整型投递次数头；首次或没有有效头时按 1 记录，仅用于观察实际重试。
     */
    private int attempt(ConsumerRecord<?, ?> record) {
        var header = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        return header == null || header.value() == null || header.value().length != Integer.BYTES
                ? 1 : java.nio.ByteBuffer.wrap(header.value()).getInt();
    }

    /**
     * 将消息身份头按 UTF-8 还原供 DLT 日志使用；外部损坏数据没有身份头时返回 unknown。
     * 缺头不妨碍保留原字节与来源 offset，不人为补造消息身份。
     */
    private String textHeader(ConsumerRecord<?, ?> record, String name) {
        var header = record.headers().lastHeader(name);
        // 非本应用产生的损坏消息可能没有业务 ID，仍保留原字节和来源 offset 供排查。
        return header == null || header.value() == null ? "unknown" : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }
}
