package com.example.learning.mq.rabbitmq.producer;

import com.example.learning.mq.common.dto.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 生产者只负责将 JSON 事件发布给 Broker，不同步执行通知业务。
 * 默认交换机（空字符串）按队列名精确路由，适合展示 Producer -> Queue -> Consumer；
 * 真实订单使用显式 Exchange 隔离队列命名和订阅者变化。
 */
@Component
@ConditionalOnProperty(name = "app.mq.rabbitmq.enabled", havingValue = "true")
public class RabbitMessageProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMessageProducer.class);
    private final RabbitTemplate template;
    private final long confirmTimeoutMs;

    public RabbitMessageProducer(RabbitTemplate template,
            @Value("${app.mq.rabbit-confirm-timeout-ms:10000}") long confirmTimeoutMs) {
        this.template = template;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    public void send(String exchange, String routingKey, MessageEvent event) {
        // 同一个 messageId 可以发布多次，但每次 Confirm correlation 必须唯一。
        var correlation = new CorrelationData(event.messageId() + ":" + UUID.randomUUID());
        LOGGER.info("[MQ-PRODUCER] middleware=RabbitMQ exchange={} routingKey={} messageId={} businessId={} result=SENDING",
                exchange, routingKey, event.messageId(), event.businessId());
        template.convertAndSend(exchange, routingKey, event, message -> {
            message.getMessageProperties().setMessageId(event.messageId());
            message.getMessageProperties().setHeader("businessId", event.businessId());
            message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            return message;
        }, correlation);
        try {
            var confirm = correlation.getFuture().get(confirmTimeoutMs, TimeUnit.MILLISECONDS);
            if (!confirm.isAck() || correlation.getReturned() != null) {
                throw new IllegalStateException("Broker NACK 或消息无法路由: " + event.messageId());
            }
            LOGGER.info("[MQ-PRODUCER] middleware=RabbitMQ exchange={} routingKey={} messageId={} businessId={} result=BROKER_CONFIRMED",
                    exchange, routingKey, event.messageId(), event.businessId());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待 RabbitMQ Confirm 被中断，发送状态待核对", exception);
        } catch (Exception exception) {
            // 超时并不能证明没发送；生产系统应以原 messageId 重试并依赖消费幂等。
            throw new IllegalStateException("RabbitMQ 发布未确认: " + event.messageId(), exception);
        }
    }
}
