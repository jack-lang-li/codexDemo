package com.example.learning.mq.rabbitmq.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * Confirm 观察生产者 -> Broker；Return 观察 Exchange -> Queue 的不可路由情况。
 * Broker 可以先 Return(NO_ROUTE)，再 Confirm(ack=true)，因此 Confirm ACK 不代表已入队。
 * 这两个回调均不能证明 Consumer 的业务成功。异常恢复应结合业务发送记录/Outbox。
 */
@Component
public class RabbitPublishListener implements RabbitTemplate.ConfirmCallback, RabbitTemplate.ReturnsCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitPublishListener.class);

    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        LOGGER.info("[MQ-PRODUCER] middleware=RabbitMQ phase=CONFIRM correlation={} ack={} cause={}",
                correlationData == null ? null : correlationData.getId(), ack, cause);
    }

    @Override
    public void returnedMessage(ReturnedMessage returned) {
        var properties = returned.getMessage().getMessageProperties();
        LOGGER.error("[MQ-PRODUCER] middleware=RabbitMQ phase=RETURN exchange={} routingKey={} messageId={} businessId={} code={} reason={}",
                returned.getExchange(), returned.getRoutingKey(), properties.getMessageId(),
                properties.getHeader("businessId"), returned.getReplyCode(), returned.getReplyText());
    }
}
