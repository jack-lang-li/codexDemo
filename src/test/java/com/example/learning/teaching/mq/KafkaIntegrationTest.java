package com.example.learning.teaching.mq;

import com.example.learning.application.LearningApplication;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.kafka.producer.KafkaMessageProducer;
import com.example.learning.teaching.mq.config.MqTeachingConfiguration;
import com.example.learning.teaching.mq.support.RealKafkaRecords;
import com.example.learning.teaching.mq.support.RealMqTestSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.example.learning.teaching.mq.config.KafkaTeachingNames.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 真实 Kafka Producer/Consumer/Group/Offset/DLT；直接使用 application.yml 的网络与认证配置。 */
@SpringBootTest(classes = LearningApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "app.mq.kafka.enabled=true", "app.mq.rabbitmq.enabled=false", "app.mq.order-listeners-enabled=false",
        "spring.kafka.admin.auto-create=true", "spring.kafka.admin.fail-fast=true", "spring.kafka.listener.auto-startup=true"})
@Import(MqTeachingConfiguration.class)
class KafkaIntegrationTest extends RealMqTestSupport {
    @Autowired KafkaMessageProducer producer;
    @Autowired KafkaTemplate<Object, Object> template;
    @Autowired KafkaProperties properties;
    @Autowired SslBundles sslBundles;
    @Autowired KafkaListenerEndpointRegistry registry;

    /**
     * 等待三个真实 Kafka 监听容器启动且完成分区分配，再开始发布测试消息。
     * 避免把消费者尚未就绪造成的等待与业务失败混淆；30 秒仍未分配则测试失败。
     */
    @BeforeAll
    void realConsumerGroupsMustBeAssignedBeforeSending() {
        await().alias("真实 Kafka 监听器完成分区分配").atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var running = registry.getListenerContainers().stream().filter(container -> container.isRunning()).toList();
            assertThat(running).hasSize(3);
            running.forEach(container -> assertThat(container.getAssignedPartitions()).isNotEmpty());
        });
    }

    /**
     * 最小 Kafka 收发：调用正式生产者发 JSON，等待真实监听器完成业务及 offset 提交。
     * 核对消息、key、分区和 offset，理解 Broker 回执与消费结果分别来自链路的不同阶段。
     */
    @Test
    void simpleJsonMessageTravelsThroughRealTopicAndListener() throws Exception {
        MessageEvent event = new MessageEvent(UUID.randomUUID().toString(), "TASK-" + UUID.randomUUID(),
                "task.submitted", "异步任务：生成订单报表", Instant.now(), 0, null);
        producer.send(SIMPLE, event).get(20, TimeUnit.SECONDS);
        var delivered = awaitDeliveries(scope(SIMPLE_GROUP, SIMPLE), event, 1).get(0);
        assertThat(delivered.event()).isEqualTo(event);
        assertThat(delivered.partition()).isBetween(0, 2);
        assertThat(delivered.offset()).isNotNegative();
        assertThat(delivered.key()).isEqualTo(event.businessId());
    }

    /**
     * 依次发送同一订单的创建与支付事件，保持业务 key 相同、messageId 不同。
     * 验证两个真实消费组各收到完整事件，且两条事件分区相同、offset 递增；顺序保证只限分区内。
     */
    @Test
    void sameOrderKeyKeepsPartitionAndTwoRealGroupsReceiveEachEvent() throws Exception {
        MessageEvent first = orderEvent("order.created");
        MessageEvent second = orderEvent(first.businessId(), "order.paid");
        producer.send(ORDER, first).get(20, TimeUnit.SECONDS);
        producer.send(ORDER, second).get(20, TimeUnit.SECONDS);
        for (String group : List.of(ORDER_GROUP, ANALYTICS_GROUP)) {
            var before = awaitDeliveries(scope(group, ORDER), first, 1).get(0);
            var after = awaitDeliveries(scope(group, ORDER), second, 1).get(0);
            assertThat(after.partition()).isEqualTo(before.partition());
            assertThat(after.offset()).isGreaterThan(before.offset());
            assertThat(probe.executions(scope(group, ORDER), first.messageId())).isEqualTo(1);
        }
        assertThat(awaitDeliveries(scope(ORDER_GROUP, ORDER), first, 1).get(0).offset())
                .isEqualTo(awaitDeliveries(scope(ANALYTICS_GROUP, ORDER), first, 1).get(0).offset());
    }

    /**
     * 显式重复发送同一事件，验证 Kafka 保存两个不同 offset，但每个消费组只真正执行业务一次。
     * 不同组分别使用真实 Redis 幂等范围，展示生产者协议幂等与业务消费去重是两件事。
     */
    @Test
    void duplicateRecordsRemainInKafkaButEachGroupExecutesBusinessOnce() throws Exception {
        MessageEvent event = orderEvent("order.created");
        producer.send(ORDER, event).get(20, TimeUnit.SECONDS);
        producer.send(ORDER, event).get(20, TimeUnit.SECONDS);
        for (String group : List.of(ORDER_GROUP, ANALYTICS_GROUP)) {
            var delivered = awaitDeliveries(scope(group, ORDER), event, 2);
            assertThat(delivered).extracting(item -> item.result()).contains("SUCCESS", "DUPLICATE_SKIPPED");
            assertThat(delivered.stream().map(item -> item.offset()).distinct().count()).isGreaterThanOrEqualTo(2);
            assertThat(probe.executions(scope(group, ORDER), event.messageId())).isEqualTo(1);
        }
    }

    /**
     * 发送必定失败的事件，观察两个组各自首次执行和两次重试后写入真实 DLT。
     * 读取 DLT 来源组、分区和 offset，再通过真实 Admin 查询恢复后的提交位置，验证没有只发死信却一直卡住源记录。
     */
    @Test
    void failedBusinessReallyRetriesPublishesDltAndCommitsRecoveredOffsets() throws Exception {
        MessageEvent event = orderEvent("demo.error");
        try (var dlt = new RealKafkaRecords(properties, sslBundles, ORDER + ".DLT")) {
            producer.send(ORDER, event).get(20, TimeUnit.SECONDS);
            var records = dlt.awaitMessage(event.messageId(), 2);
            assertThat(records).extracting(record -> text(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP).value()))
                    .contains(ORDER_GROUP, ANALYTICS_GROUP);
            for (String group : List.of(ORDER_GROUP, ANALYTICS_GROUP)) {
                assertThat(probe.attempts(scope(group, ORDER), event.messageId())).isEqualTo(3);
                assertThat(probe.executions(scope(group, ORDER), event.messageId())).isZero();
            }
            try (Admin admin = Admin.create(properties.buildAdminProperties(sslBundles))) {
                for (var record : records) {
                    String group = text(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP).value());
                    int partition = ByteBuffer.wrap(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION).value()).getInt();
                    long offset = ByteBuffer.wrap(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value()).getLong();
                    await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                        var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                        assertThat(committed.get(new TopicPartition(ORDER, partition))).isNotNull();
                        assertThat(committed.get(new TopicPartition(ORDER, partition)).offset()).isGreaterThan(offset);
                    });
                }
            }
        }
    }

    /**
     * 使用真实 KafkaTemplate 发送不可解析的 JSON，触发消费者反序列化错误。
     * 独立客户端核对 DLT 原始字节，验证错误数据没有被 JSON 序列化器再次包装而失去故障原文。
     */
    @Test
    void invalidJsonIsRecoveredAsOriginalBytesInRealDlt() throws Exception {
        String identity = "malformed-" + UUID.randomUUID();
        byte[] brokenJson = identity.getBytes(StandardCharsets.UTF_8);
        try (var dlt = new RealKafkaRecords(properties, sslBundles, ORDER + ".DLT")) {
            template.send(ORDER, identity, brokenJson).get(20, TimeUnit.SECONDS);
            assertThat(dlt.awaitMessage(identity, 2)).allSatisfy(record -> assertThat(record.value()).isEqualTo(brokenJson));
        }
    }

    /**
     * 拼出与教学消费者一致的幂等及结果查询范围：不同 group、不同 topic 分别统计。
     * 只用于定位真实回调结果，不参与 Kafka 消息分发或 offset 提交。
     */
    private String scope(String group, String topic) {
        return "kafka:" + group + ":" + topic;
    }

    /**
     * 按 Kafka 字符串头约定用 UTF-8 解码来源消费组等信息，供真实 DLT 断言使用。
     */
    private String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
