package com.example.learning.teaching.mq.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import java.util.ArrayList;
import java.util.List;
import static com.example.learning.teaching.mq.config.RabbitTeachingNames.*;

/** 任务、Topic、Fanout、故意失败及 MANUAL ACK 仅供测试导入，不进入服务制品。 */
@TestConfiguration(proxyBeanMethods = false)
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "app.mq.rabbitmq.enabled", havingValue = "true")
public class RabbitTeachingConfiguration {
    /**
     * 一次声明基础队列、Direct、Topic、Fanout 及死信资源，由 RabbitAdmin 在真实服务器应用。
     * 订单与任务分别绑定精确路由，Topic 绑定 * 和 #，Fanout 为邮件、短信、审计各绑定独立队列。
     */
    @Bean
    Declarables rabbitDemoTopology() {
        var direct = new DirectExchange(DIRECT, true, false);
        var dlx = new DirectExchange(DLX, true, false);
        var topic = new TopicExchange(TOPIC, true, false);
        var fanout = new FanoutExchange(FANOUT, true, false);
        List<Declarable> topology = new ArrayList<>(List.of(direct, dlx, topic, fanout));
        Queue created = businessQueue(CREATED);
        Queue dlq = QueueBuilder.durable(DLQ).build();
        Queue process = businessQueue(PROCESS);
        Queue star = businessQueue(TOPIC_STAR);
        Queue hash = businessQueue(TOPIC_HASH);
        // DLQ 不再指回业务队列，避免永久失败消息在两者之间无限循环。
        topology.addAll(List.of(businessQueue(SIMPLE), created, dlq, process, star, hash,
                BindingBuilder.bind(created).to(direct).with(CREATED_KEY),
                BindingBuilder.bind(dlq).to(dlx).with(DEAD_KEY),
                new Binding(PROCESS, Binding.DestinationType.QUEUE, DIRECT, PROCESS_KEY, null),
                BindingBuilder.bind(star).to(topic).with("order.*"),
                BindingBuilder.bind(hash).to(topic).with("order.#")));
        // * 匹配恰好一个词段；# 匹配零个或多个词段。order.payment.completed 只进 hash 队列。
        for (String name : List.of(EMAIL, SMS, AUDIT)) {
            Queue queue = businessQueue(name);
            Binding binding = BindingBuilder.bind(queue).to(fanout);
            topology.addAll(List.of(queue, binding));
        }
        return new Declarables(topology);
    }

    /**
     * 为指定练习业务队列配置持久化及共同的死信出口，拒绝且不重入队时由 Broker 转给 DLX。
     * 不设置 TTL；重试次数由监听容器控制，消息寿命与重试次数是不同概念。
     */
    private Queue businessQueue(String name) {
        // Retry 是应用中的再次执行；DLX 是 Broker 的转发入口；DLQ 是存放失败证据的队列。
        // TTL 是消息/队列寿命，不等于重试次数。本例不对业务消息设 TTL，防止学习时意外过期。
        return QueueBuilder.durable(name).deadLetterExchange(DLX).deadLetterRoutingKey(DEAD_KEY).build();
    }

    /**
     * 创建仅供手动确认案例使用的真实监听容器，继承 YAML 连接、并发和预取设置。
     * 改为 MANUAL 并清空 AUTO 重试拦截器，由监听方法自行选择 ACK/NACK/Reject，避免重复确认 deliveryTag。
     */
    @Bean
    SimpleRabbitListenerContainerFactory rabbitManualContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
        var factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        // 手动消费者自行 NACK/Reject，清空 AUTO 的重试拦截器以免重复确认同一 deliveryTag。
        factory.setAdviceChain();
        return factory;
    }
}
