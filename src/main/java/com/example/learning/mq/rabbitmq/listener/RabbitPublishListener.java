package com.example.learning.mq.rabbitmq.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * Confirm 观察生产者 -> Broker；Return 观察 Exchange -> Queue 的不可路由情况。
 * Broker 可以先 Return(NO_ROUTE)，再 Confirm(ack=true)，因此 Confirm ACK 不代表已入队。
 * 这两个回调均不能证明 Consumer 的业务成功。异常恢复应结合业务发送记录/Outbox。
 */
@Component
@ConditionalOnProperty(name = "app.mq.rabbitmq.enabled", havingValue = "true")
public class RabbitPublishListener implements RabbitTemplate.ConfirmCallback, RabbitTemplate.ReturnsCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitPublishListener.class);

    /**
     * 记录 Broker 对本次发布的确认或否认，correlationData 用于关联某一次发送尝试。
     * ack 仅说明 Broker 的发布确认结果，不能据此认定已路由入队或消费者业务成功。
     */
    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        LOGGER.info("[MQ-PRODUCER] middleware=RabbitMQ phase=CONFIRM correlation={} ack={} cause={}",
                correlationData == null ? null : correlationData.getId(), ack, cause);
    }

    /**
     * 记录 mandatory 消息没有任何队列接收的情况，输出交换机、路由键和 Broker 原因。
     * 此回调与 Confirm 可以先后都发生；生产者还会检查 CorrelationData 中的 Return 来向调用方报错。
     */
    @Override
    public void returnedMessage(ReturnedMessage returned) {
        var properties = returned.getMessage().getMessageProperties();
        LOGGER.error("[MQ-PRODUCER] middleware=RabbitMQ phase=RETURN exchange={} routingKey={} messageId={} businessId={} code={} reason={}",
                returned.getExchange(), returned.getRoutingKey(), properties.getMessageId(),
                properties.getHeader("businessId"), returned.getReplyCode(), returned.getReplyText());
    }
}
