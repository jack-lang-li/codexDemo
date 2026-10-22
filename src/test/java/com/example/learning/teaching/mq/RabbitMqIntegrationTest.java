package com.example.learning.teaching.mq;

import com.example.learning.application.LearningApplication;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.rabbitmq.producer.RabbitMessageProducer;
import com.example.learning.teaching.mq.config.MqTeachingConfiguration;
import com.example.learning.teaching.mq.support.RealMqTestSupport;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static com.example.learning.teaching.mq.config.RabbitTeachingNames.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.awaitility.Awaitility.await;

/**
 * 直接运行即可：application.yml 的真实地址/账号 -> 真实 Producer -> RabbitMQ -> @RabbitListener。
 * 不需要 MQ_PRACTICE，不使用 Mockito/MockMvc，不直接调用 Consumer。
 * 教学资源使用 learning.practice.*，不与正在运行的正式订单消费者竞争。
 */
@SpringBootTest(classes = LearningApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "app.mq.rabbitmq.enabled=true", "app.mq.kafka.enabled=false", "app.mq.order-listeners-enabled=false",
        "spring.rabbitmq.dynamic=true", "spring.rabbitmq.listener.simple.auto-startup=true"})
@Import(MqTeachingConfiguration.class)
class RabbitMqIntegrationTest extends RealMqTestSupport {
    @Autowired RabbitMessageProducer producer;
    @Autowired RabbitTemplate template;

    /**
     * 开始案例前实际建立到 YAML 中 RabbitMQ 的连接，并检查真实 Channel 已打开。
     * 连接或认证失败直接使测试失败，避免把未连接服务器误当作测试通过。
     */
    @BeforeAll
    void brokerConnectionMustBeReal() {
        Boolean connected = template.execute(channel -> channel.isOpen());
        assertThat(connected).isTrue();
    }

    /**
     * 最小收发案例：生产者经默认交换机按队列名发送 JSON，真实监听器接收后执行业务并记录结果。
     * 测试等待监听器异步回调，不直接调用消费者；消息内容与发送时相同才算收发成功。
     */
    @Test
    void simpleQueueReceivesRealJsonMessage() {
        MessageEvent event = new MessageEvent(UUID.randomUUID().toString(), "TASK-" + UUID.randomUUID(),
                "task.submitted", "异步任务：生成订单报表", Instant.now(), 0, null);
        producer.send("", SIMPLE, event);
        var delivered = awaitDeliveries("rabbit:" + SIMPLE, event, 1).get(0);
        assertThat(delivered.event()).isEqualTo(event);
        assertThat(delivered.result()).isEqualTo("SUCCESS");
    }

    /**
     * 订单创建案例：生产者发送到 Direct 交换机，按 order.created 精确路由到订单队列。
     * 等待消费者完成真实 Redis 幂等及 basicAck 后的结果，并核对消息中的订单金额快照。
     */
    @Test
    void directExchangeRoutesOrderAndConsumerSendsManualAck() {
        MessageEvent event = orderEvent("order.created");
        producer.send(DIRECT, CREATED_KEY, event);
        // 此结果仅在监听器真实 basicAck 成功后记录。
        assertThat(awaitDeliveries("rabbit:" + CREATED, event, 1).get(0).result()).isEqualTo("SUCCESS");
        assertThat(event.order().amount()).isEqualByComparingTo("39.80");
    }

    /**
     * 用两个真实路由键验证 Topic 规则：order.paid 同时匹配 order.* 和 order.#，
     * order.payment.completed 只匹配 order.#；断言来自服务器实际投递，不在 Java 中模拟路由。
     */
    @Test
    void topicStarAndHashAreRoutedByRealExchange() {
        MessageEvent shortKey = orderEvent("order.paid");
        producer.send(TOPIC, "order.paid", shortKey);
        awaitDeliveries("rabbit:" + TOPIC_STAR, shortKey, 1);
        awaitDeliveries("rabbit:" + TOPIC_HASH, shortKey, 1);

        MessageEvent deepKey = orderEvent("order.payment.completed");
        producer.send(TOPIC, "order.payment.completed", deepKey);
        awaitDeliveries("rabbit:" + TOPIC_HASH, deepKey, 1);
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                assertThat(probe.deliveries("rabbit:" + TOPIC_STAR, deepKey.messageId())).isEmpty());
    }

    /**
     * 注册通知广播案例：同一事件由 Fanout 复制到邮件、短信、审计三个独立队列。
     * 每个真实消费者各执行一次日志业务，展示一个事件触发多个后续任务；不调用外部短信或邮件服务。
     */
    @Test
    void fanoutBroadcastsToThreeIndependentRealQueues() {
        MessageEvent event = new MessageEvent(UUID.randomUUID().toString(), "U-" + UUID.randomUUID(),
                "user.registered", "注册成功：触发邮件、短信与审计", Instant.now(), 0, null);
        producer.send(FANOUT, "", event);
        for (String queue : new String[]{EMAIL, SMS, AUDIT}) {
            awaitDeliveries("rabbit:" + queue, event, 1);
            assertThat(probe.executions("rabbit:" + queue, event.messageId())).isEqualTo(1);
        }
    }

    /**
     * 显式向真实队列发送两次相同 messageId，验证两次均收到，但只有第一次执行核心业务。
     * 第二次因真实 Redis 的 DONE 标记跳过并正常确认，避免重复投递造成重复业务。
     */
    @Test
    void duplicateDeliveryUsesRealRedisAndExecutesBusinessOnce() {
        MessageEvent event = orderEvent("order.created");
        producer.send(DIRECT, CREATED_KEY, event);
        producer.send(DIRECT, CREATED_KEY, event);
        assertThat(awaitDeliveries("rabbit:" + CREATED, event, 2)).extracting(delivery -> delivery.result())
                .contains("SUCCESS", "DUPLICATE_SKIPPED");
        assertThat(probe.executions("rabbit:" + CREATED, event.messageId())).isEqualTo(1);
    }

    /**
     * 主动制造业务异常，观察 AUTO 容器首次执行加两次重试后拒绝消息。
     * 从真实 DLX 的观察队列读取死信，核对三次尝试及 Broker 写入的 x-death。
     */
    @Test
    void autoRetryExhaustionReachesRealDeadLetterExchange() {
        assertRealDeadLetter(PROCESS, PROCESS_KEY, "demo.error", 3);
    }

    /**
     * 主动制造普通业务异常，验证 MANUAL 消费者只执行一次后 basicNack(requeue=false)。
     * 消息通过真实 DLX 进入死信，避免 requeue=true 在永久失败时形成快速循环消费。
     */
    @Test
    void manualNackDoesNotRequeueForever() {
        assertRealDeadLetter(CREATED, CREATED_KEY, "demo.error", 1);
    }

    /**
     * 主动制造无效业务事件，验证 MANUAL 消费者通过 basicReject(requeue=false) 拒绝。
     * 检查服务器死信而非只检查方法调用，说明无效事件没有被成功 ACK 或无限重投。
     */
    @Test
    void manualRejectAlsoReachesDeadLetterQueue() {
        assertRealDeadLetter(CREATED, CREATED_KEY, "demo.reject", 1);
    }

    /**
     * 向存在的交换机发送一个无绑定的随机路由键，触发真实 mandatory Return。
     * 即使 Broker 随后 Confirm ACK，生产者仍向调用方报告不可路由，不能把确认接收等同于入队。
     */
    @Test
    void unroutableMessageIsReturnedEvenWhenBrokerConfirmsPublish() {
        MessageEvent event = orderEvent("order.created");
        assertThrows(IllegalStateException.class, () -> producer.send(DIRECT, "unbound." + UUID.randomUUID(), event));
    }

    /**
     * 为失败案例建立临时独占队列并绑定真实 DLX，只观察本次发送产生的死信副本。
     * 检查消息 ID、原队列和 rejected 原因，再核对真实消费者的尝试次数；结束只删除临时队列。
     * @param sourceQueue 预期失败来源队列
     * @param key 发送到 Direct 交换机时的路由键
     * @param type 触发业务异常的测试事件类型
     * @param attempts 预期总尝试次数，包含首次消费
     */
    private void assertRealDeadLetter(String sourceQueue, String key, String type, int attempts) {
        // 临时复制队列接收 DLX 的同一份死信；不清空、不读取移除永久 DLQ 中的失败证据。
        String copy = template.execute(channel -> {
            String queue = channel.queueDeclare("", false, true, true, null).getQueue();
            channel.queueBind(queue, DLX, DEAD_KEY);
            return queue;
        });
        try {
            MessageEvent event = orderEvent(type);
            producer.send(DIRECT, key, event);
            Message dead = template.receive(copy, 30000);
            assertThat(dead).as("服务器实际经 DLX 转发的死信").isNotNull();
            assertThat(dead.getMessageProperties().getMessageId()).isEqualTo(event.messageId());
            assertThat(dead.getMessageProperties().getXDeathHeader()).anySatisfy(death -> {
                assertThat(death.get("queue").toString()).isEqualTo(sourceQueue);
                assertThat(death.get("reason").toString()).isEqualTo("rejected");
            });
            assertThat(probe.attempts("rabbit:" + sourceQueue, event.messageId())).isEqualTo(attempts);
            assertThat(probe.executions("rabbit:" + sourceQueue, event.messageId())).isZero();
        } finally {
            template.execute(channel -> { channel.queueDelete(copy); return null; });
        }
    }
}
