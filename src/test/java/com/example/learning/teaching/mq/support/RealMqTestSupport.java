package com.example.learning.teaching.mq.support;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.common.dto.OrderCreatedEvent;
import com.example.learning.teaching.mq.common.service.MqConsumptionProbe;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 真实服务器测试的公共断言；连接失败就失败，没有跳过、内嵌 Broker 或客户端替身。 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class RealMqTestSupport {
    @Autowired protected MqConsumptionProbe probe;
    @Autowired protected StringRedisTemplate redis;

    /**
     * 测试前向 YAML 对应的 Redis 实际发送 PING，确保消费者依赖的幂等存储可用。
     * 连接必须成功且返回 PONG；不跳过失败，也不改成本地 Map 代替 Redis。
     */
    @BeforeAll
    void realRedisMustBeAvailableForConsumerIdempotency() {
        try (var connection = redis.getConnectionFactory().getConnection()) {
            assertThat(connection.ping()).as("真实消费者依赖 YAML 中的 Redis 幂等存储").isEqualTo("PONG");
        }
    }

    /**
     * 生成随机订单号的消息样本，隔离本次测试与历史消息、旧幂等标记。
     * 这里只准备待发送的数据；真正的下单及金额规则由 OrderMqIntegrationTest 单独验证。
     */
    protected MessageEvent orderEvent(String type) {
        return orderEvent("O-REAL-" + UUID.randomUUID(), type);
    }

    /**
     * 为指定订单号构造一个新事件，供同 key 多事件的分区顺序案例复用。
     * 每次使用新 messageId，快照与外层身份一致；39.80 是两件商品的固定样本金额，不是业务计算替身。
     */
    protected MessageEvent orderEvent(String orderId, String type) {
        String messageId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        var order = new OrderCreatedEvent(messageId, orderId, "U-REAL-TEST",
                new BigDecimal("39.80"), now, type);
        return new MessageEvent(messageId, orderId, type, "真实 MQ 订单事件", now, 0, order);
    }

    /**
     * 最多等待 30 秒，直到真实监听器记录本消息的预期投递次数，再返回只读结果快照。
     * 通过轮询等异步消费完成，不使用固定长睡眠，也不手工触发消费者；超时直接断言失败。
     */
    protected List<MqConsumptionProbe.Delivery> awaitDeliveries(String subscriber, MessageEvent event, int count) {
        await().alias("真实监听器消费 " + subscriber + " / " + event.messageId())
                .atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
                        assertThat(probe.deliveries(subscriber, event.messageId())).hasSizeGreaterThanOrEqualTo(count));
        return probe.deliveries(subscriber, event.messageId());
    }
}
