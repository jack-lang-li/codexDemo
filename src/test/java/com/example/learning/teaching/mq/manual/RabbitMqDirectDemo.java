package com.example.learning.teaching.mq.manual;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.common.dto.OrderCreatedEvent;
import com.example.learning.mq.rabbitmq.producer.RabbitMessageProducer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.awaitility.Awaitility.await;

/**
 * RabbitMQ 第二课：显式 Direct 交换机通过 routingKey 与 Binding 精确匹配，把订单事件发给后续处理队列。
 * IDEA 单独运行 consumeMessages 保持监听，再运行 sendMatchingMessage；sendUnroutableMessage 演示无绑定时的返回。
 * 复用第一课的最小客户端装配，不加载订单 Service 或 Redis；新增资源与第一课、正式业务均隔离。
 * 类名以 Demo 结尾，普通 mvn test 不进入持续监听；所有方法均可在 IDEA 独立点击运行。
 */
@SpringBootTest(classes = RabbitMqDirectDemo.DemoConfiguration.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "app.mq.rabbitmq.enabled=true", "spring.rabbitmq.dynamic=true",
        "spring.rabbitmq.template.mandatory=true", "spring.rabbitmq.listener.simple.concurrency=1",
        "spring.rabbitmq.listener.simple.max-concurrency=1"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RabbitMqDirectDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqDirectDemo.class);
    private static final String EXCHANGE = "learning.idea.rabbit.direct.exchange";
    private static final String QUEUE = "learning.idea.rabbit.direct.created.queue";
    private static final String ROUTING_KEY = "order.created";
    private static final String LISTENER = "ideaRabbitDirectConsumer";

    @Autowired RabbitMessageProducer producer;
    @Autowired RabbitTemplate template;
    @Autowired RabbitListenerEndpointRegistry registry;
    @Autowired DemoConfiguration configuration;

    // 默认持续监听；命令行核验可设置正数秒数，实际收到一条消息即结束，超时失败。
    @Value("${mq.demo.receive-timeout-seconds:0}")
    long receiveTimeoutSeconds;

    /**
     * 发送绑定匹配的真实订单事件：Exchange 和 routingKey 都明确指定，生产者无需知道消费者的队列名。
     * 路由规则存在服务器的 Binding 中；同一交换机下其他 routingKey 不会自动进入此队列。
     * 本进程不启动监听器，可以先发后收，也可以让另一个消费者测试进程先运行。
     */
    @Test
    void sendMatchingMessage() {
        MessageEvent event = orderCreatedEvent();
        producer.send(EXCHANGE, ROUTING_KEY, event);
        LOGGER.info("[IDEA-RABBIT-DIRECT-SENT] exchange={} routingKey={} messageId={} orderId={}",
                EXCHANGE, ROUTING_KEY, event.messageId(), event.businessId());
    }

    /**
     * 向真实交换机发送一个没有 Binding 的随机 routingKey，观察 mandatory Return 而非消费者日志。
     * Broker 可以 Confirm ACK，但消息没有入队；已有生产者检查 CorrelationData 的 Return 并向调用方抛异常。
     * 同时核对真实 ReturnsCallback 的 312/NO_ROUTE 回执、路由键和 messageId，发送超时不能冒充此案例成功。
     * 此测试预期收到异常，因此测试变绿表示正确识别了不可路由，并不代表这条消息已消费。
     */
    @Test
    void sendUnroutableMessage() {
        MessageEvent event = orderCreatedEvent();
        String wrongKey = "unbound." + UUID.randomUUID();
        var returned = new AtomicReference<ReturnedMessage>();
        template.setReturnsCallback(returned::set);
        assertThrows(IllegalStateException.class, () -> producer.send(EXCHANGE, wrongKey, event));
        // Return 是异步回调，等待真实回执到达后再读，不能将发送线程返回与回调线程处理完毕等同。
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(returned.get()).as("Broker 实际返回不可路由回执").isNotNull());
        assertThat(returned.get().getReplyCode()).isEqualTo(312);
        assertThat(returned.get().getExchange()).isEqualTo(EXCHANGE);
        assertThat(returned.get().getRoutingKey()).isEqualTo(wrongKey);
        assertThat(returned.get().getMessage().getMessageProperties().getMessageId()).isEqualTo(event.messageId());
        LOGGER.info("[IDEA-RABBIT-DIRECT-RETURNED] exchange={} routingKey={} messageId={} replyCode={} replyText={}",
                EXCHANGE, wrongKey, event.messageId(), returned.get().getReplyCode(), returned.get().getReplyText());
    }

    /**
     * 只启动本课的订单队列监听器，保持窗口运行，再从另一个 IDEA 测试窗口执行发送方法。
     * 第一课的监听器虽然复用配置时会注册，但 autoStartup=false，此处不启动它也不抢第一课消息。
     * 默认一直监听；限时核验只等一次真实接收，finally 停止监听并由测试上下文释放连接。
     * @throws InterruptedException 等待中断时交给测试框架处理
     */
    @Test
    void consumeMessages() throws InterruptedException {
        var container = registry.getListenerContainer(LISTENER);
        assertThat(container).as("Direct 独立消费者已注册").isNotNull();
        try {
            container.start();
            LOGGER.info("[IDEA-RABBIT-DIRECT-LISTENING] queue={}，另一个窗口运行 sendMatchingMessage；Stop 结束", QUEUE);
            if (receiveTimeoutSeconds > 0) {
                assertThat(configuration.firstMessage.await(receiveTimeoutSeconds, TimeUnit.SECONDS))
                        .as("真实 Direct 路由的消息已被消费者接收").isTrue();
            } else {
                new CountDownLatch(1).await();
            }
        } finally {
            container.stop();
        }
    }

    /**
     * 直接构造订单事件样本，不调用下单服务；这里只学习路由，金额 39.80 是便于阅读的固定快照。
     * 外层 messageId/businessId 与内层 eventId/orderId 保持一致，演示结构化订单事件而不是随意拼字符串。
     */
    private MessageEvent orderCreatedEvent() {
        String messageId = UUID.randomUUID().toString();
        String orderId = "IDEA-DIRECT-ORDER-" + UUID.randomUUID();
        Instant now = Instant.now();
        var order = new OrderCreatedEvent(messageId, orderId, "U-IDEA", new BigDecimal("39.80"), now, ROUTING_KEY);
        return new MessageEvent(messageId, orderId, ROUTING_KEY, "订单创建：交给后续任务消费者", now, 0, order);
    }

    /**
     * 复用第一课的 RabbitMQ 连接、JSON 转换及真实生产者，只增加 Direct 的拓扑与接收函数。
     * 不重复写连接配置，不扫描业务 Service；没有新增应用 main，仍是 test 内的最小容器。
     */
    @Configuration(proxyBeanMethods = false)
    @Import(RabbitMqProducerConsumerDemo.DemoConfiguration.class)
    static class DemoConfiguration {
        private final CountDownLatch firstMessage = new CountDownLatch(1);

        /**
         * 在真实服务器声明持久 Direct 交换机、队列和 order.created 精确绑定。
         * 交换机负责路由、队列保存待消费消息、Binding 表达订阅规则；这三者是不同职责。
         */
        @Bean
        Declarables ideaDirectTopology() {
            var exchange = new DirectExchange(EXCHANGE, true, false);
            var queue = QueueBuilder.durable(QUEUE).build();
            return new Declarables(exchange, queue, BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY));
        }

        /**
         * 服务器按 Binding 路由入队后触发的真实回调，打印结构化订单内容，正常返回后 AUTO ACK。
         * 队列只绑定 order.created，因此错误路由键的消息不会触发此方法；失败业务应抛异常而非提前确认。
         */
        @RabbitListener(id = LISTENER, queues = QUEUE, autoStartup = "false", ackMode = "AUTO")
        void receive(MessageEvent event) {
            LOGGER.info("[IDEA-RABBIT-DIRECT-RECEIVED] queue={} messageId={} orderId={} amount={} content={}",
                    QUEUE, event.messageId(), event.businessId(), event.order().amount(), event.content());
            firstMessage.countDown();
        }
    }
}
