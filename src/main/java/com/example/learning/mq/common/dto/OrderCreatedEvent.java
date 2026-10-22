package com.example.learning.mq.common.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * 订单的不可变事件快照，而非直接把领域对象作为外部协议。
 * eventId 是事件身份，orderId 是聚合业务键，二者不可混用；金额复用原订单计算结果。
 */
public record OrderCreatedEvent(
        /* 事件唯一 ID，与外层 messageId 一致。 */ String eventId,
        /* 订单 ID，也是 Kafka 的分区 key。 */ String orderId,
        /* 下单用户 ID。 */ String userId,
        /* 下单时总金额快照，不使用浮点数表示货币。 */ BigDecimal amount,
        /* 事件产生时间，统一使用 UTC Instant。 */ Instant createTime,
        /* 事件语义及未来协议演进的识别依据。 */ String eventType) {
    /**
     * 校验订单快照的必填字段，避免消费者在后续处理中才发现缺失数据。
     * 金额由订单领域规则计算，快照构造器不重复计算；与外层消息的身份一致性由 OrderEventHandler 校验。
     * @throws NullPointerException 任一快照字段为 null
     */
    public OrderCreatedEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(userId, "userId");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(createTime, "createTime");
        Objects.requireNonNull(eventType, "eventType");
    }
}
