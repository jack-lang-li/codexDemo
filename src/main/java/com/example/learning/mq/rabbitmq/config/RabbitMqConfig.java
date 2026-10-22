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

    /**
     * 声明正式订单的持久化 Direct 交换机、业务队列、死信交换机和死信队列，并建立精确绑定。
     * RabbitAdmin 连接服务器时应用这些声明；业务消息重试耗尽被拒绝后，经 DLX 转入 DLQ 保留。
     */
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

    /**
     * 复用 Boot 的 ObjectMapper 将 MessageEvent 转成 JSON，并支持 Instant 时间字段。
     * 限定受信任的消息 DTO 包，避免根据外部类型头任意反序列化应用中的其他类型。
     */
    @Bean
    Jackson2JsonMessageConverter rabbitJsonConverter(ObjectMapper mapper) {
        // 复用 Boot 的时间模块；限制受信任包，拒绝任意类型反序列化。
        return new Jackson2JsonMessageConverter(mapper, "com.example.learning.mq.common.dto");
    }

    /**
     * 构建真实 RabbitMQ 发送客户端，继承 YAML 中的连接、超时及确认设置。
     * 设置 JSON 转换器和 mandatory，并注册 Confirm/Return 回调，使“Broker 接收”和“无法路由”都可观察。
     */
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

    /**
     * 定义本地消费重试耗尽后的处理：记录失败，再拒绝当前消息且不重新入原队列。
     * 依赖队列的 DLX 绑定保存死信；若这里只吞异常，AUTO 容器会认为成功并确认，失去失败证据。
     */
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

    /**
     * 创建正式消费者默认容器，沿用 YAML 的并发、预取及有限重试配置。
     * AUTO 表示业务正常返回后由 Spring ACK；异常抛出则按重试和恢复策略处理，不是收到即确认。
     */
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
