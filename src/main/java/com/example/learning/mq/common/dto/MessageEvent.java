package com.example.learning.mq.common.dto;

import java.time.Instant;
import java.util.Objects;

/**
 * 业务与两种中间件复用的 JSON 信封。业务身份不随发送重试改变，方便消费端识别重复。
 * record 可由项目现有 Jackson 版本直接序列化/反序列化，无需新引入 JSON 依赖。
 */
public record MessageEvent(
        /* 消息唯一 ID；重复发送演示必须复用此值。 */ String messageId,
        /* 业务唯一键，如 orderId；不能替代每个事件的 messageId。 */ String businessId,
        /* 事件类型，如 order.created；故障注入只在测试实现。 */ String eventType,
        /* 可选业务描述；日志不应输出敏感业务正文。 */ String content,
        /* 服务端生成的事件时间。 */ Instant createTime,
        /* 初始为 0；框架内存重试不会改写原 JSON，请结合重试日志/header 观察实际次数。 */ int retryCount,
        /* 订单事件的结构化载荷；任务、注册消息可以没有订单。 */ OrderCreatedEvent order) {
    /**
     * 在显式构造及 Jackson 反序列化时校验消息信封，阻止身份为空或重试计数为负的消息进入业务。
     * order 对通用任务可以为空，订单专用消费者会进一步校验订单快照。
     * @throws IllegalArgumentException 必填身份为空白或 retryCount 为负
     * @throws NullPointerException 描述或事件时间为 null
     */
    public MessageEvent {
        if (messageId == null || messageId.isBlank() || businessId == null || businessId.isBlank()
                || eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("messageId/businessId/eventType must not be blank");
        }
        Objects.requireNonNull(content, "content");
        Objects.requireNonNull(createTime, "createTime");
        if (retryCount < 0) {
            throw new IllegalArgumentException("retryCount must not be negative");
        }
    }
}
