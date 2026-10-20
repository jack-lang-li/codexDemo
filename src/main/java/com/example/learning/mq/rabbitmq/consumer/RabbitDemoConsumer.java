package com.example.learning.mq.rabbitmq.consumer;

import com.example.learning.mq.common.dto.MessageDemoDTO;
import com.example.learning.mq.common.service.DemoBusinessService;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static com.example.learning.mq.rabbitmq.constant.RabbitMqNames.*;

/**
 * Consumer 从 Queue 获取消息，调用业务服务后再确认。
 * 普通任务/主题/广播用 Spring AUTO + 有限重试；订单创建队列演示 MANUAL ACK。
 * 不把异步业务交给其他线程后就返回，否则 AUTO 会在业务真正成功之前确认。
 */
@Component
public class RabbitDemoConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitDemoConsumer.class);
    private final DemoBusinessService business;

    public RabbitDemoConsumer(DemoBusinessService business) {
        this.business = business;
    }

    @RabbitListener(queues = {SIMPLE, PROCESS, TOPIC_STAR, TOPIC_HASH, EMAIL, SMS, AUDIT})
    public void automatic(MessageDemoDTO event, Message raw) {
        handle(raw.getMessageProperties().getConsumerQueue(), event);
        // 仅正常返回才由容器 ACK；异常一定向外抛出，以便重试并最终 reject 到 DLX。
    }

    @RabbitListener(queues = CREATED, containerFactory = "rabbitManualContainerFactory")
    public void manual(MessageDemoDTO event, Message raw, Channel channel) throws IOException {
        long tag = raw.getMessageProperties().getDeliveryTag();
        // deliveryTag 只在当前 Channel 有效，multiple=false 避免误确认尚未完成的其他消息。
        try {
            handle(CREATED, event);
        } catch (IllegalArgumentException exception) {
            channel.basicReject(tag, false);
            return;
        } catch (RuntimeException exception) {
            // requeue=true 会回到原队列；如果业务一直失败，会形成快速死循环。
            // 本例 requeue=false 进入配置好的 DLX；没有 DLX 则会丢弃。有限重试见 PROCESS 的 AUTO 案例。
            channel.basicNack(tag, false, false);
            return;
        }
        // 核心业务+幂等 DONE 均完成后才 ACK。若 ACK 本身失败，不再尝试 NACK，交由连接恢复重投。
        channel.basicAck(tag, false);
    }

    @RabbitListener(queues = DLQ, autoStartup = "${demo.mq.dlq-listener-enabled:false}")
    public void deadLetter(Message raw) {
        // 默认关闭以保留管理台的 DLQ 消息；开启时只记录隔离信息，不调用失败业务、不自动重放。
        LOGGER.warn("[MQ-CONSUMER] middleware=RabbitMQ queue={} messageId={} businessId={} x-death={} result=DLQ_OBSERVED durationMs=0",
                DLQ, raw.getMessageProperties().getMessageId(), raw.getMessageProperties().getHeader("businessId"),
                raw.getMessageProperties().getHeader("x-death"));
    }

    private void handle(String queue, MessageDemoDTO event) {
        long start = System.nanoTime();
        try {
            String result = business.process("rabbit:" + queue, event);
            LOGGER.info("[MQ-CONSUMER] middleware=RabbitMQ queue={} messageId={} businessId={} result={} durationMs={}",
                    queue, event.messageId(), event.businessId(), result, (System.nanoTime() - start) / 1_000_000);
        } catch (RuntimeException exception) {
            LOGGER.error("[MQ-CONSUMER] middleware=RabbitMQ queue={} messageId={} businessId={} result=FAILED durationMs={}",
                    queue, event.messageId(), event.businessId(), (System.nanoTime() - start) / 1_000_000, exception);
            throw exception;
        }
    }
}
