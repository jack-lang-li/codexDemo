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

    @Bean
    KafkaAdmin.NewTopics orderTopics(@Value("${app.mq.kafka-replicas:1}") int replicas) {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(ORDER).partitions(3).replicas(replicas).build(),
                TopicBuilder.name(ORDER + ".DLT").partitions(3).replicas(replicas).build());
    }

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

    @Bean
    KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

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
