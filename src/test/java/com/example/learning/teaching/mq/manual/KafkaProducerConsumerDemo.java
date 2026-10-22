package com.example.learning.teaching.mq.manual;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.kafka.producer.KafkaMessageProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IDEA 中分别运行生产者和消费者的 Kafka 实验，无需部署服务、创建订单或启动 Redis。
 * 点击 consumeMessages 后持续监听，再单独点击 sendMessage；控制台会打印真正的内容、分区和 offset。
 * 独立装配只加载现有 YAML 中的 Kafka 客户端和认证参数，既不扫描业务 Service，也不连接 RabbitMQ。
 * 类名以 Demo 结尾，使常规 mvn test 不自动进入持续监听；在 IDEA 直接运行指定 @Test 方法即可。
 */
@SpringBootTest(classes = KafkaProducerConsumerDemo.DemoConfiguration.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "app.mq.kafka.enabled=true", "spring.kafka.admin.auto-create=true", "spring.kafka.admin.fail-fast=true",
        "spring.kafka.consumer.enable-auto-commit=false", "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.listener.ack-mode=manual_immediate"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class KafkaProducerConsumerDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerConsumerDemo.class);
    private static final String TOPIC = "learning.idea.kafka.messages";
    private static final String GROUP = "learning-idea-kafka-group";
    private static final String LISTENER = "ideaKafkaConsumer";

    @Autowired KafkaMessageProducer producer;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired DemoConfiguration configuration;

    // IDEA 默认 0：持续监听，点击 Stop 结束。正数用于命令行验证首次收消息，超时直接失败。
    @Value("${mq.demo.receive-timeout-seconds:0}")
    long receiveTimeoutSeconds;

    /**
     * 生产者入口：点击测试箭头，即向 YAML 配置的真实 Kafka 发送一条 MessageEvent JSON。
     * businessId 是 Kafka key；可修改 content 观察消息内容，messageId 每次随机生成用于日志对照。
     * 等待 Broker 回执再结束；本进程不会启动消费者，消费由另一个独立运行的 consumeMessages 处理。
     * @throws Exception 发送失败或 20 秒内没有确认，直接让测试失败，不模拟成功
     */
    @Test
    void sendMessage() throws Exception {
        var event = new MessageEvent(UUID.randomUUID().toString(), "IDEA-KAFKA-DEMO",
                "demo.message", "你好 Kafka，这是在 IDEA 点击测试发送的真实消息", Instant.now(), 0, null);
        producer.send(TOPIC, event).get(20, TimeUnit.SECONDS);
        LOGGER.info("[IDEA-KAFKA-SENT] topic={} messageId={} content={}", TOPIC, event.messageId(), event.content());
    }

    /**
     * 消费者入口：只启动 Kafka 监听器并保持进程存活，不发消息、不调用订单或其他业务 Service。
     * 在另一运行窗口点击 sendMessage 后，receive 会异步打印真实记录并提交 offset；可重复点击发送。
     * 默认一直监听，点击 IDEA Stop 结束；正数超时参数仅供自动核验第一次接收，收不到则失败。
     * finally 停止容器，DirtiesContext 释放客户端；固定 group 从已提交进度继续，新组首次按 earliest 定位。
     * @throws InterruptedException 等待中断时向测试框架传播
     */
    @Test
    void consumeMessages() throws InterruptedException {
        var container = registry.getListenerContainer(LISTENER);
        assertThat(container).as("已注册独立 Kafka 消费者").isNotNull();
        try {
            container.start();
            LOGGER.info("[IDEA-KAFKA-LISTENING] topic={} group={}，保持本测试运行；在另一个窗口运行 sendMessage，点击 Stop 结束", TOPIC, GROUP);
            if (receiveTimeoutSeconds > 0) {
                assertThat(configuration.firstMessage.await(receiveTimeoutSeconds, TimeUnit.SECONDS))
                        .as("限定时间内从真实 Kafka 收到消息").isTrue();
            } else {
                new CountDownLatch(1).await();
            }
        } finally {
            container.stop();
        }
    }

    /**
     * 本文件的最小 Kafka 测试装配，仅导入 Kafka 自动配置、SSL 参数支持及项目现有生产者。
     * 不扫描业务、不创建第二个应用 main；测试自身即可建立真实连接，并在服务器声明学习 Topic。
     * firstMessage 仅用于限时等待真实回调，不替换消费者、消息、Broker 或提交句柄。
     */
    @Configuration(proxyBeanMethods = false)
    @EnableKafka
    @ImportAutoConfiguration({SslAutoConfiguration.class, KafkaAutoConfiguration.class})
    @Import(KafkaMessageProducer.class)
    static class DemoConfiguration {
        private final CountDownLatch firstMessage = new CountDownLatch(1);

        /**
         * 声明独立的真实学习 Topic，单分区足以观察最小收发，副本数沿用当前 MQ 配置。
         * 生产者与消费者启动时均可幂等声明；名称与正式订单 Topic 隔离，不清空历史记录。
         */
        @Bean
        KafkaAdmin.NewTopics ideaKafkaTopic(@Value("${app.mq.kafka-replicas:1}") int replicas) {
            return new KafkaAdmin.NewTopics(TopicBuilder.name(TOPIC).partitions(1).replicas(replicas).build());
        }

        /**
         * 真正的消费者回调：Kafka 客户端拉取到记录后由 Spring 调用，打印正文和真实 partition/offset/key。
         * 日志业务完成后手动提交 offset；后续可将日志替换为自己的处理函数，失败应抛错而非提前提交。
         * autoStartup=false 让发送测试只负责发送，只有 consumeMessages 才会加入独立的学习消费组。
         */
        @KafkaListener(id = LISTENER, topics = TOPIC, groupId = GROUP, autoStartup = "false")
        void receive(ConsumerRecord<String, MessageEvent> record, Acknowledgment acknowledgment) {
            MessageEvent event = record.value();
            LOGGER.info("[IDEA-KAFKA-RECEIVED] topic={} partition={} offset={} key={} messageId={} content={} createTime={}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    event.messageId(), event.content(), event.createTime());
            acknowledgment.acknowledge();
            firstMessage.countDown();
        }
    }
}
