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

    /**
     * 注入真实 Redis 幂等服务，让两种 MQ 的消费者复用同一套重复消息处理规则。
     */
    public OrderEventHandler(RedisMessageIdempotencyService idempotency) {
        this.idempotency = idempotency;
    }

    /**
     * 处理消费者收到的订单创建事件：先校验事件身份与订单快照，再在 Redis 幂等保护下记录订单。
     * 当前后续业务只记录日志，实际库存、通知或统计逻辑可接在幂等回调中；此处不代表已实现外部系统。
     * @param subscriber 订阅者身份；不同后续业务需分别传入，避免一个业务抑制另一个业务
     * @param event 从 MQ 反序列化得到的完整订单消息
     * @return SUCCESS 表示本次执行业务；DUPLICATE_SKIPPED 表示该订阅者已处理过
     * @throws RuntimeException 数据不合法、Redis 不可用或业务失败时抛出，由监听容器重试或隔离
     */
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
