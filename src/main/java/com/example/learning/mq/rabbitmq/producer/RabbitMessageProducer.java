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

    /**
     * 注入真实 RabbitTemplate，并读取等待 Broker Confirm 的最长时间。
     * 发送超时属于结果不确定，调用方应使用原 messageId 核对或重试，不能直接生成新事件重复业务。
     */
    public RabbitMessageProducer(RabbitTemplate template,
            @Value("${app.mq.rabbit-confirm-timeout-ms:10000}") long confirmTimeoutMs) {
        this.template = template;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    /**
     * 把结构化事件以持久化 JSON 发布给 Exchange，并同步等待本次发布的 Broker Confirm。
     * 同时检查 Return：即使 Confirm 为 ACK，无匹配队列时仍视为失败；消费端业务是否完成另由 ACK 表达。
     * @param exchange 交换机名；空字符串表示 RabbitMQ 默认交换机
     * @param routingKey 路由键；使用默认交换机时填写目标队列名
     * @param event 待发事件；重发同一事件应保留 messageId
     * @throws IllegalStateException 发布失败、超时、被 NACK、无法路由或等待被中断
     */
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
