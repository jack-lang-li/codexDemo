package com.example.learning.mq.kafka.config;

import com.example.learning.mq.common.dto.MessageEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

import static com.example.learning.mq.kafka.constant.KafkaNames.*;

/**
 * 适配当前 Spring Kafka 3.3.x：DefaultErrorHandler + DeadLetterPublishingRecoverer。
 * Topic/DLT 显式声明，3 个分区展示并行与 key 路由；单机学习副本数为 1，不具备节点容灾。
 * 生产需规划分区、副本、min.insync.replicas、权限和容量，不能依赖 Broker 自动建 Topic。
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.mq.kafka.enabled", havingValue = "true")
public class KafkaConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConfig.class);

    /**
     * 通过 KafkaAdmin 声明正式订单 Topic 和对应 DLT，二者分区数一致便于保留原分区。
     * replicas 来自现有配置，默认 1 适用于单 Broker 学习环境；副本数不能超过可用 Broker 数。
     */
    @Bean
    KafkaAdmin.NewTopics orderTopics(@Value("${app.mq.kafka-replicas:1}") int replicas) {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(ORDER).partitions(3).replicas(replicas).build(),
                TopicBuilder.name(ORDER + ".DLT").partitions(3).replicas(replicas).build());
    }

    /**
     * 继承 YAML 的服务器、认证及生产参数，按消息类型选用真实客户端的序列化器。
     * 正常 MessageEvent 序列化为 JSON；反序列化故障保留的 byte[] 原样写入 DLT，避免破坏排错原文。
     */
    @Bean
    ProducerFactory<Object, Object> kafkaProducerFactory(KafkaProperties properties, SslBundles sslBundles,
            ObjectMapper mapper) {
        // 正常 DTO -> JSON；反序列化失败时 Recoverer 提取原 byte[]，必须原样送 DLT，不能再 JSON 编码。
        // 只使用 JsonSerializer 会破坏故障原文，因此按值类型选择序列化器；业务仍只发送结构化 DTO。
        var values = new DelegatingByTypeSerializer(Map.of(
                MessageEvent.class, new JsonSerializer<MessageEvent>(mapper),
                byte[].class, new ByteArraySerializer()));
        var keys = new DelegatingByTypeSerializer(Map.of(String.class, new StringSerializer(),
                byte[].class, new ByteArraySerializer()));
        return new DefaultKafkaProducerFactory<>(properties.buildProducerProperties(sslBundles), keys, values);
    }

    /**
     * 将生产者工厂包装为 Spring Kafka 发送入口，供正常业务及死信恢复共用连接和认证参数。
     * 返回的模板执行真实网络发送，不创建内嵌 Broker。
     */
    @Bean
    KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * 将无法继续处理的记录发往“原 Topic.DLT”的同一分区，并保留来源及业务身份头。
     * 必须确认 DLT 写入成功；写入失败继续抛错，让源记录保持可重投，避免先提交 offset 造成消息丢失。
     */
    @Bean
    DeadLetterPublishingRecoverer kafkaDeadLetterRecoverer(KafkaTemplate<Object, Object> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        recoverer.setHeadersFunction((record, exception) -> {
            var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
            if (record.value() instanceof MessageEvent event) {
                headers.add(MESSAGE_ID_HEADER, event.messageId().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                headers.add(BUSINESS_ID_HEADER, event.businessId().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            return headers;
        });
        // 必须确认 DLT 发布结果；DLT 写入失败时抛错，原记录保持可重投，不能先提交源 offset。
        recoverer.setFailIfSendResultIsError(true);
        return recoverer;
    }

    /**
     * 为可重试业务异常配置固定间隔、有限次数的重试，耗尽后交给死信发布器。
     * retries 是额外重试次数，默认 2 加首次共 3 次；不可重试的协议/反序列化异常可直接隔离。
     * 配合 MANUAL_IMMEDIATE，在 DLT 发布成功后提交恢复位置，避免每次启动反复处理同一失败记录。
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer,
            @Value("${app.mq.kafka-retry-interval-ms:1000}") long interval,
            @Value("${app.mq.kafka-retry-count:2}") long retries) {
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(interval, retries));
        // 2 次重试 + 首次 = 3 次尝试。反序列化/协议错误默认属于不可恢复异常，可直接 DLT。
        handler.setRetryListeners((record, exception, attempt) ->
                LOGGER.error("[MQ-CONSUMER] middleware=Kafka phase=RETRY topic={} partition={} offset={} key={} attempt={}",
                        record.topic(), record.partition(), record.offset(), record.key(), attempt, exception));
        // 与 MANUAL_IMMEDIATE 配套：仅在 Recoverer 成功发布 DLT 后提交失败记录之后的位置。
        handler.setCommitRecovered(true);
        return handler;
    }

    /**
     * 装配正式 Kafka 监听容器，继承 YAML 的消费者、并发及手动提交配置。
     * 注册统一重试/DLT 错误处理器并启用投递次数头，监听方法成功后再主动提交 offset。
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory, DefaultErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setDeliveryAttemptHeader(true);
        return factory;
    }

    /**
     * 提供独立的死信观察容器；其处理出错就停止容器等待排查。
     * 不套用业务 Topic 的 DLT 转发逻辑，防止生成 .DLT.DLT 并无限转发。
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaDltContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory);
        // 隔离观察者失败就停止等待人工处理，不再派生 .DLT.DLT 导致死信无限转发。
        factory.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
        return factory;
    }
}
