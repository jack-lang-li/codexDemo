package com.example.learning.mq.rabbitmq.consumer;

import com.example.learning.application.order.OrderEventHandler;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.rabbitmq.constant.RabbitMqNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** 同一 LearningApplication 服务内的订单订阅；业务成功返回后 AUTO ACK，异常交给有限重试/DLX。 */
@Component
@ConditionalOnProperty(name = {"app.mq.rabbitmq.enabled", "app.mq.order-listeners-enabled"}, havingValue = "true")
public class RabbitOrderConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitOrderConsumer.class);
    private final OrderEventHandler handler;

    public RabbitOrderConsumer(OrderEventHandler handler) {
        this.handler = handler;
    }

    @RabbitListener(queues = RabbitMqNames.CREATED)
    public void onOrderCreated(MessageEvent event) {
        long start = System.nanoTime();
        try {
            String result = handler.onOrderCreated("rabbit:order-followup", event);
            LOGGER.info("[MQ-CONSUMER] middleware=RabbitMQ queue={} messageId={} businessId={} result={} durationMs={}",
                    RabbitMqNames.CREATED, event.messageId(), event.businessId(), result,
                    (System.nanoTime() - start) / 1_000_000);
        } catch (RuntimeException exception) {
            LOGGER.error("[MQ-CONSUMER] middleware=RabbitMQ queue={} messageId={} businessId={} result=FAILED durationMs={}",
                    RabbitMqNames.CREATED, event == null ? null : event.messageId(), event == null ? null : event.businessId(),
                    (System.nanoTime() - start) / 1_000_000, exception);
            throw exception;
        }
    }
}
