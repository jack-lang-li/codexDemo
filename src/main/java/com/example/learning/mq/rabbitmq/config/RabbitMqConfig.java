package com.example.learning.mq.rabbitmq.config;

import com.example.learning.mq.rabbitmq.listener.RabbitPublishListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateConfigurer;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import static com.example.learning.mq.rabbitmq.constant.RabbitMqNames.*;

/**
 * RabbitAdmin 声明持久化 Exchange/Queue/Binding，不要求手工在管理台创建。
 * Exchange 接收发布并决定路由；Queue 缓冲待消费消息；Binding 是路由规则。
 * Producer 面对事件与 Exchange，增加订阅者只需要增加绑定，减少对具体队列的耦合。
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.mq.rabbitmq.enabled", havingValue = "true")
public class RabbitMqConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqConfig.class);

    @Bean
    Declarables orderRabbitTopology() {
        var direct = new DirectExchange(DIRECT, true, false);
        var dlx = new DirectExchange(DLX, true, false);
        Queue orders = QueueBuilder.durable(CREATED).deadLetterExchange(DLX).deadLetterRoutingKey(DEAD_KEY).build();
        Queue deadLetters = QueueBuilder.durable(DLQ).build();
        return new Declarables(direct, dlx, orders, deadLetters,
                BindingBuilder.bind(orders).to(direct).with(CREATED_KEY),
                BindingBuilder.bind(deadLetters).to(dlx).with(DEAD_KEY));
    }

    @Bean
    Jackson2JsonMessageConverter rabbitJsonConverter(ObjectMapper mapper) {
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
    MessageRecoverer rabbitRetryRecoverer() {
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

}
