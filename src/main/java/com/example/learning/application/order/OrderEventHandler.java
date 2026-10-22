package com.example.learning.application.order;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.common.service.RedisMessageIdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 消费后的业务函数，供本服务的 RabbitMQ/Kafka 消费者调用。
 * 当前没有真实通知、库存或统计系统，先记录结构化订单快照；后续业务应在此边界扩展。
 * 不包含故意失败、重复发送等教学分支；这些仅存在于 src/test。
 */
@Service
public class OrderEventHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderEventHandler.class);
    private final RedisMessageIdempotencyService idempotency;

    public OrderEventHandler(RedisMessageIdempotencyService idempotency) {
        this.idempotency = idempotency;
    }

    public String onOrderCreated(String subscriber, MessageEvent event) {
        if (event == null || event.order() == null || !"order.created".equals(event.eventType())
                || !event.messageId().equals(event.order().eventId())
                || !event.businessId().equals(event.order().orderId())) {
            throw new IllegalArgumentException("订单事件缺少有效的订单快照或身份不一致");
        }
        boolean processed = idempotency.execute(subscriber, event.messageId(), () ->
                LOGGER.info("[ORDER-EVENT] subscriber={} messageId={} orderId={} amount={} result=PROCESSED",
                        subscriber, event.messageId(), event.order().orderId(), event.order().amount()));
        return processed ? "SUCCESS" : "DUPLICATE_SKIPPED";
    }
}
