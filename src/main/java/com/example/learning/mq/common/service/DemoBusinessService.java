package com.example.learning.mq.common.service;

import com.example.learning.mq.common.dto.MessageDemoDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 消费后的业务边界：以日志模拟订单后续任务、通知和统计，方便观察失败、重复和重放。
 * MQ 把耗时后续任务移出请求线程，但接入方也必须接受最终一致、重复投递和异步失败。
 */
@Service
public class DemoBusinessService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoBusinessService.class);
    private final RedisMessageIdempotencyService idempotency;

    public DemoBusinessService(RedisMessageIdempotencyService idempotency) {
        this.idempotency = idempotency;
    }

    public String process(String subscriber, MessageDemoDTO message) {
        boolean processed = idempotency.execute(subscriber, message.messageId(), () -> {
            if ("demo.error".equals(message.eventType())) {
                throw new IllegalStateException("故意失败，用于观察 Retry -> DLQ/DLT");
            }
            LOGGER.info("[MQ-BUSINESS] subscriber={} messageId={} businessId={} eventType={} result=PROCESSED",
                    subscriber, message.messageId(), message.businessId(), message.eventType());
        });
        return processed ? "SUCCESS" : "DUPLICATE_SKIPPED";
    }
}
