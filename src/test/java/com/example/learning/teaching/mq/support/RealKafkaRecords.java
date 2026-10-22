package com.example.learning.teaching.mq.support;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 独立的真实 Kafka 观察客户端：继承 YAML 的地址、SASL/TLS 等连接参数。
 * 使用手动 assign 和随机组，从创建观察器时的日志末端开始，不修改业务组的 offset。
 * 读取 DLT 的原始字节可验证损坏 JSON 没有被二次编码，绝不使用假 ConsumerRecord。
 */
public final class RealKafkaRecords implements AutoCloseable {
    private final KafkaConsumer<String, byte[]> consumer;

    /**
     * 创建继承 YAML 连接与认证参数的原生 KafkaConsumer，以 byte[] 读取指定 Topic 的原始记录。
     * 手动分配全部分区并定位到当前末端，必须在发送前构造；随机组且不提交，不影响业务组进度。
     * 构造失败立即关闭客户端，防止真实网络连接和线程泄漏。
     */
    public RealKafkaRecords(KafkaProperties properties, SslBundles sslBundles, String topic) {
        var configuration = properties.buildConsumerProperties(sslBundles);
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, "learning-real-observer-" + UUID.randomUUID());
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumer = new KafkaConsumer<>(configuration);
        try {
            var partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10)).stream()
                    .map(info -> new TopicPartition(topic, info.partition())).toList();
            assertThat(partitions).as("真实服务器必须已存在目标 Topic").isNotEmpty();
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            for (var partition : partitions) {
                consumer.position(partition, Duration.ofSeconds(10));
            }
        } catch (RuntimeException | AssertionError exception) {
            consumer.close(Duration.ofSeconds(3));
            throw exception;
        }
    }

    /**
     * 持续 poll 真实 Broker，筛选本次唯一消息身份的记录，达到预期份数即返回，最多等待 30 秒。
     * 同时用于正常事件和损坏 JSON 的唯一标记；随机身份避免历史数据误使断言通过。
     * @param messageId 当前消息 ID 或损坏消息中的唯一标记
     * @param count 预期份数，例如两个消费组失败时各写一条 DLT
     * @return 实际 Kafka 原始记录，包含服务器分区、offset 和 headers
     */
    public List<ConsumerRecord<String, byte[]>> awaitMessage(String messageId, int count) {
        List<ConsumerRecord<String, byte[]>> matched = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (matched.size() < count && System.nanoTime() < deadline) {
            for (var record : consumer.poll(Duration.ofMillis(200))) {
                if (record.value() != null && new String(record.value(), StandardCharsets.UTF_8).contains(messageId)) {
                    matched.add(record);
                }
            }
        }
        assertThat(matched).as("真实 Broker 中 messageId=" + messageId).hasSizeGreaterThanOrEqualTo(count);
        return matched;
    }

    /**
     * 在 try-with-resources 结束时关闭真实消费者，最多等待 3 秒释放连接和客户端资源。
     * 不删除 Topic、消息或消费组，也不修改业务提交位置。
     */
    @Override
    public void close() {
        consumer.close(Duration.ofSeconds(3));
    }
}
