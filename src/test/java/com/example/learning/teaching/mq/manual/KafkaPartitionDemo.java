package com.example.learning.teaching.mq.manual;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.kafka.producer.KafkaMessageProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka 第二课：同 key、不同 key 与显式指定分区三种发送方式，观察真实分区、offset 与消费线程。
 * 单独启动 consumeMessages 持续监听，再任选发送方法；只复用第一课客户端，不依赖订单或 Redis。
 * 新建独立三分区 Topic，三个消费线程加入同一学习组；分区内有序不等于整个 Topic 全局有序。
 * 类名以 Demo 结尾，普通 mvn test 不进入持续监听；IDEA 点击具体 @Test 方法即可运行。
 */
@SpringBootTest(classes = KafkaPartitionDemo.DemoConfiguration.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "app.mq.kafka.enabled=true", "spring.kafka.admin.auto-create=true", "spring.kafka.admin.fail-fast=true",
        "spring.kafka.consumer.enable-auto-commit=false", "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.listener.ack-mode=manual_immediate"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class KafkaPartitionDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionDemo.class);
    private static final String TOPIC = "learning.idea.kafka.partition.messages";
    private static final String GROUP = "learning-idea-kafka-partition-group";
    private static final String LISTENER = "ideaKafkaPartitionConsumer";

    @Autowired KafkaMessageProducer producer;
    @Autowired KafkaTemplate<Object, Object> template;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired DemoConfiguration configuration;

    // IDEA 默认持续监听；命令行限时核验时配合 expected-messages 等到指定数量真实回调后结束。
    @Value("${mq.demo.receive-timeout-seconds:0}")
    long receiveTimeoutSeconds;

    /**
     * 连续发送同一订单 key 的五条不同事件，每条等待 Broker 回执后再发送下一条。
     * 在分区数和默认分区器不变时，这些 key 相同的记录应落在同一分区，offset 按发送顺序递增。
     * content 带序号，messageId 各不相同；这是五个独立事件，不是重复消息去重案例。
     * @throws Exception 网络发送失败或 Broker 确认超时即测试失败
     */
    @Test
    void sendSameKeyMessages() throws Exception {
        for (int sequence = 1; sequence <= 5; sequence++) {
            MessageEvent event = message("IDEA-SAME-ORDER", "同一订单事件，序号=" + sequence);
            producer.send(TOPIC, event).get(20, TimeUnit.SECONDS);
            LOGGER.info("[IDEA-KAFKA-PARTITION-SENT] mode=SAME_KEY sequence={} key={} messageId={}",
                    sequence, event.businessId(), event.messageId());
        }
    }

    /**
     * 用十个不同订单 key 发送消息，观察 Broker 回执和消费日志中的分区分布及三个消费线程。
     * 不同 key 可能进入不同分区，也可能碰巧落在同一分区；少量样本不保证三个分区都有消息或均匀分布。
     * 跨分区日志先后不代表全局业务顺序，实际顺序应在各分区内分别观察 offset。
     * @throws Exception 网络发送失败或 Broker 确认超时即测试失败
     */
    @Test
    void sendDifferentKeyMessages() throws Exception {
        for (int sequence = 1; sequence <= 10; sequence++) {
            MessageEvent event = message("IDEA-ORDER-" + UUID.randomUUID(), "不同订单事件，序号=" + sequence);
            producer.send(TOPIC, event).get(20, TimeUnit.SECONDS);
            LOGGER.info("[IDEA-KAFKA-PARTITION-SENT] mode=DIFFERENT_KEYS sequence={} key={} messageId={}",
                    sequence, event.businessId(), event.messageId());
        }
    }

    /**
     * 显式把一条消息发到编号 2 的分区，展示 send(topic, partition, key, value) 的真实用法。
     * 指定分区后由 partition 决定目的地，key 不再负责选择分区；分区必须存在且编号从 0 开始。
     * 检查 Broker 回执中的实际分区，避免把方法调用成功误认为已按目标分区发送。
     * @throws Exception 目标分区不可用、网络发送失败或确认超时即测试失败
     */
    @Test
    void sendToSpecifiedPartition() throws Exception {
        MessageEvent event = message("IDEA-EXPLICIT-PARTITION", "明确发送到 Partition 2");
        var result = template.send(TOPIC, 2, event.businessId(), event).get(20, TimeUnit.SECONDS);
        var metadata = result.getRecordMetadata();
        assertThat(metadata.partition()).isEqualTo(2);
        LOGGER.info("[IDEA-KAFKA-PARTITION-SENT] mode=EXPLICIT topic={} partition={} offset={} key={} messageId={}",
                metadata.topic(), metadata.partition(), metadata.offset(), event.businessId(), event.messageId());
    }

    /**
     * 仅启动本课的三线程 Kafka 消费容器，让另一个进程可以分别运行三个发送入口。
     * 默认持续监听；限时核验时只有接收到 expected-messages 指定数量才通过，收不够则超时失败。
     * 同组每个分区同一时刻分配给一个成员；启动完成后分区分配需要时间，消息会先保留在 Topic 中。
     * @throws InterruptedException 等待中断时交给测试框架处理
     */
    @Test
    void consumeMessages() throws InterruptedException {
        var container = registry.getListenerContainer(LISTENER);
        assertThat(container).as("三分区独立消费者已注册").isNotNull();
        try {
            container.start();
            LOGGER.info("[IDEA-KAFKA-PARTITION-LISTENING] topic={} group={}，另一个窗口运行发送方法；Stop 结束", TOPIC, GROUP);
            if (receiveTimeoutSeconds > 0) {
                assertThat(configuration.receivedMessages.await(receiveTimeoutSeconds, TimeUnit.SECONDS))
                        .as("已从真实 Kafka 接收到预期数量的消息").isTrue();
            } else {
                new CountDownLatch(1).await();
            }
        } finally {
            container.stop();
        }
    }

    /**
     * 构造学习用事件，businessId 由各个发送案例指定并直接用作 Kafka key，messageId 每次随机生成。
     * 此处只准备消息，不创建订单、不假装执行库存或支付，content 用于把发送序号与消费日志对应起来。
     */
    private MessageEvent message(String businessId, String content) {
        return new MessageEvent(UUID.randomUUID().toString(), businessId, "demo.partition", content, Instant.now(), 0, null);
    }

    /**
     * 复用第一课的连接、序列化与生产者，只声明本课的三分区 Topic 和三线程监听方法。
     * 观察实际回调的倒计数仅用于命令行限时核验，IDEA 默认始终监听，不依赖观察器或业务 Service。
     */
    @Configuration(proxyBeanMethods = false)
    @Import(KafkaProducerConsumerDemo.DemoConfiguration.class)
    static class DemoConfiguration {
        private final CountDownLatch receivedMessages;

        /**
         * 指定限时核验要等到的消息数量，默认 1；例如验证全部发送方法可设为 16（5 + 10 + 1）。
         * 此参数只影响核验何时结束，不改变真实分区路由、客户端消费或 offset 提交行为。
         */
        DemoConfiguration(@Value("${mq.demo.expected-messages:1}") int expectedMessages) {
            if (expectedMessages < 1) {
                throw new IllegalArgumentException("mq.demo.expected-messages 必须大于零");
            }
            receivedMessages = new CountDownLatch(expectedMessages);
        }

        /**
         * 在真实 Kafka 声明独立三分区 Topic，以便观察 key 分区、显式选分区及同组并行消费。
         * 副本数沿用 app.mq.kafka-replicas；不修改原业务 Topic 的分区或删除历史记录。
         */
        @Bean
        KafkaAdmin.NewTopics ideaPartitionTopic(@Value("${app.mq.kafka-replicas:1}") int replicas) {
            return new KafkaAdmin.NewTopics(TopicBuilder.name(TOPIC).partitions(3).replicas(replicas).build());
        }

        /**
         * 真正的分区消费回调：记录分区、offset、key、线程及内容，业务日志完成后手动提交 offset。
         * 三个线程消费同一组的不同分区；同 key 顺序需要在同分区内比较，不能按多个线程的打印先后推断全局顺序。
         */
        @KafkaListener(id = LISTENER, topics = TOPIC, groupId = GROUP, concurrency = "3", autoStartup = "false")
        void receive(ConsumerRecord<String, MessageEvent> record, Acknowledgment acknowledgment) {
            MessageEvent event = record.value();
            LOGGER.info("[IDEA-KAFKA-PARTITION-RECEIVED] topic={} partition={} offset={} key={} messageId={} thread={} content={}",
                    record.topic(), record.partition(), record.offset(), record.key(), event.messageId(),
                    Thread.currentThread().getName(), event.content());
            acknowledgment.acknowledge();
            receivedMessages.countDown();
        }
    }
}
