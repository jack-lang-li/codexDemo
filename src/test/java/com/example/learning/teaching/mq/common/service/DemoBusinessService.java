package com.example.learning.teaching.mq.common.service;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.common.service.RedisMessageIdempotencyService;
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
    private final MqConsumptionProbe probe;

    /**
     * 注入正式的 Redis 幂等服务，以及仅记录真实回调结果的测试观察器。
     * 真实 MQ 和 Redis 都不替换；观察器只是让异步消费者的实际执行结果可被测试断言。
     */
    public DemoBusinessService(RedisMessageIdempotencyService idempotency, MqConsumptionProbe probe) {
        this.idempotency = idempotency;
        this.probe = probe;
    }

    /**
     * 模拟收到事件后需要异步完成的订单后续任务、通知或统计，本例实际业务效果是日志记录。
     * 先记录一次真实消费尝试，再用 Redis 保护业务；完成才累加执行次数，重复消息返回跳过结果。
     * demo.error 和 demo.reject 专供主动观察重试、NACK/Reject、死信，故意抛错不等于使用 Mock。
     * @param subscriber 独立业务订阅身份，包含队列或消费组
     * @param message 待处理的真实 MQ 消息
     * @return SUCCESS 或 DUPLICATE_SKIPPED，供消费者决定正常确认并记录结果
     * @throws RuntimeException 故障事件或 Redis 异常，交给消费者及容器处理
     */
    public String process(String subscriber, MessageEvent message) {
        probe.attempted(subscriber, message);
        boolean processed = idempotency.execute(subscriber, message.messageId(), () -> {
            if ("demo.reject".equals(message.eventType())) {
                throw new IllegalArgumentException("真实手动 ACK 案例：拒绝无效业务事件");
            }
            if ("demo.error".equals(message.eventType())) {
                throw new IllegalStateException("故意失败，用于观察 Retry -> DLQ/DLT");
            }
            LOGGER.info("[MQ-BUSINESS] subscriber={} messageId={} businessId={} eventType={} result=PROCESSED",
                    subscriber, message.messageId(), message.businessId(), message.eventType());
            probe.businessExecuted(subscriber, message);
        });
        return processed ? "SUCCESS" : "DUPLICATE_SKIPPED";
    }
}
