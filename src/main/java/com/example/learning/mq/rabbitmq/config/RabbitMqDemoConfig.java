package com.example.learning.mq.rabbitmq.config;

import com.example.learning.mq.rabbitmq.listener.RabbitPublishListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateConfigurer;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

import static com.example.learning.mq.rabbitmq.constant.RabbitMqNames.*;

/**
 * RabbitAdmin 声明持久化 Exchange/Queue/Binding，不要求手工在管理台创建。
 * Exchange 接收发布并决定路由；Queue 缓冲待消费消息；Binding 是路由规则。
 * Producer 面对事件与 Exchange，增加订阅者只需要增加绑定，减少对具体队列的耦合。
 */
@Configuration(proxyBeanMethods = false)
public class RabbitMqDemoConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqDemoConfig.class);

    @Bean
    Declarables rabbitDemoTopology() {
        var direct = new DirectExchange(DIRECT, true, false);
        var topic = new TopicExchange(TOPIC, true, false);
        var fanout = new FanoutExchange(FANOUT, true, false);
        var dlx = new DirectExchange(DLX, true, false);
        List<Declarable> topology = new ArrayList<>(List.of(direct, topic, fanout, dlx));
        Queue created = businessQueue(CREATED);
        Queue process = businessQueue(PROCESS);
        Queue star = businessQueue(TOPIC_STAR);
        Queue hash = businessQueue(TOPIC_HASH);
        // DLQ 不再指回业务队列，避免永久失败消息在两者之间无限循环。
        Queue dlq = QueueBuilder.durable(DLQ).build();
        topology.addAll(List.of(businessQueue(SIMPLE), created, process, star, hash, dlq,
                BindingBuilder.bind(created).to(direct).with(CREATED_KEY),
                BindingBuilder.bind(process).to(direct).with(PROCESS_KEY),
                BindingBuilder.bind(star).to(topic).with("order.*"),
                BindingBuilder.bind(hash).to(topic).with("order.#"),
                BindingBuilder.bind(dlq).to(dlx).with(DEAD_KEY)));
        // * 匹配恰好一个词段；# 匹配零个或多个词段。order.payment.completed 只进 hash 队列。
        for (String name : List.of(EMAIL, SMS, AUDIT)) {
            Queue queue = businessQueue(name);
            Binding binding = BindingBuilder.bind(queue).to(fanout);
            topology.addAll(List.of(queue, binding));
        }
        return new Declarables(topology);
    }

    private Queue businessQueue(String name) {
        // Retry 是应用中的再次执行；DLX 是 Broker 的转发入口；DLQ 是存放失败证据的队列。
        // TTL 是消息/队列寿命，不等于重试次数。本例不对业务消息设 TTL，防止学习时意外过期。
        return QueueBuilder.durable(name).deadLetterExchange(DLX).deadLetterRoutingKey(DEAD_KEY).build();
    }

    @Bean
    Jackson2JsonMessageConverter rabbitDemoJsonConverter(ObjectMapper mapper) {
        // 复用 Boot 的时间模块；限制受信任包，拒绝任意类型反序列化。
        return new Jackson2JsonMessageConverter(mapper, "com.example.learning.mq.common.dto");
    }

    @Bean
    RabbitTemplate rabbitTemplate(RabbitTemplateConfigurer configurer, ConnectionFactory connectionFactory,
            Jackson2JsonMessageConverter converter, RabbitPublishListener listener) {
        var template = new RabbitTemplate();
        configurer.configure(template, connectionFactory);
        template.setMessageConverter(converter);
        template.setMandatory(true); // 无队列接收时触发 Return，否则可能直接丢弃且 Confirm 仍 ACK。
        template.setConfirmCallback(listener);
        template.setReturnsCallback(listener);
        return template;
    }

    @Bean
    MessageRecoverer rabbitDemoRecoverer() {
        return (message, cause) -> {
            LOGGER.error("[MQ-CONSUMER] middleware=RabbitMQ phase=RETRY_EXHAUSTED queue={} messageId={} businessId={} result=REJECT_TO_DLX",
                    message.getMessageProperties().getConsumerQueue(), message.getMessageProperties().getMessageId(),
                    message.getMessageProperties().getHeader("businessId"), cause);
            // 抛出 AmqpRejectAndDontRequeueException，让容器 reject(requeue=false)，Broker 按 DLX 规则投递。
            // 不能简单吞异常，否则 AUTO 容器会 ACK，消息便不会进入死信队列。
            new RejectAndDontRequeueRecoverer().recover(message, cause);
        };
    }

    @Bean
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
        var factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        // AUTO 是 Spring 业务正常返回才 ACK，并非协议级 autoAck=true（Spring 中后者叫 NONE）。
        factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
        return factory;
    }

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
