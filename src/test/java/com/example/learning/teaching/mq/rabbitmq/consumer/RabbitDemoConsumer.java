package com.example.learning.teaching.mq.rabbitmq.consumer;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.teaching.mq.common.service.DemoBusinessService;
import com.example.learning.teaching.mq.common.service.MqConsumptionProbe;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static com.example.learning.teaching.mq.config.RabbitTeachingNames.*;

/**
 * Consumer 从 Queue 获取消息，调用业务服务后再确认。
 * 普通任务/主题/广播用 Spring AUTO + 有限重试；订单创建队列演示 MANUAL ACK。
 * 不把异步业务交给其他线程后就返回，否则 AUTO 会在业务真正成功之前确认。
 */
@Component
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "app.mq.rabbitmq.enabled", havingValue = "true")
public class RabbitDemoConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitDemoConsumer.class);
    private final DemoBusinessService business;
    private final MqConsumptionProbe probe;

    /**
     * 注入消费后的日志业务及观察器，消费结果必须由真实 RabbitMQ 推送触发后才记录。
     */
    public RabbitDemoConsumer(DemoBusinessService business, MqConsumptionProbe probe) {
        this.business = business;
        this.probe = probe;
    }

    /**
     * 接收基础任务、Topic 路由、Fanout 广播及重试队列消息，从原始属性识别实际来源队列。
     * 调用日志业务与真实 Redis 幂等后返回；正常返回才由容器 AUTO ACK，异常由有限重试及 DLX 接管。
     */
    @RabbitListener(queues = {SIMPLE, PROCESS, TOPIC_STAR, TOPIC_HASH, EMAIL, SMS, AUDIT})
    public void automatic(MessageEvent event, Message raw) {
        String queue = raw.getMessageProperties().getConsumerQueue();
        String result = handle(queue, event);
        probe.received("rabbit:" + queue, event, result, null, null, null);
        // 仅正常返回才由容器 ACK；异常一定向外抛出，以便重试并最终 reject 到 DLX。
    }

    /**
     * 接收 Direct 订单消息，业务完成后对当前 Channel 的单条 deliveryTag 执行 basicAck。
     * 无效事件 basicReject(false)，普通失败 basicNack(false,false)，均不重入原队列而进入 DLX。
     * ACK 本身失败向外传播，不再尝试二次 NACK；连接恢复后可能重投，交由业务幂等处理。
     * @throws IOException 真实确认操作的网络或通道错误
     */
    @RabbitListener(queues = CREATED, containerFactory = "rabbitManualContainerFactory")
    public void manual(MessageEvent event, Message raw, Channel channel) throws IOException {
        long tag = raw.getMessageProperties().getDeliveryTag();
        String result;
        // deliveryTag 只在当前 Channel 有效，multiple=false 避免误确认尚未完成的其他消息。
        try {
            result = handle(CREATED, event);
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
        probe.received("rabbit:" + CREATED, event, result, null, null, null);
    }

    /**
     * 可选的死信观察监听器，只记录 Broker 写入的 x-death 等排错信息，不自动重放失败业务。
     * 默认关闭以便在管理台保留 DLQ 消息；显式开启后正常返回会由 AUTO 容器确认并移除死信。
     */
    @RabbitListener(queues = DLQ, autoStartup = "${teaching.mq.dlq-listener-enabled:false}")
    public void deadLetter(Message raw) {
        // 默认关闭以保留管理台的 DLQ 消息；开启时只记录隔离信息，不调用失败业务、不自动重放。
        LOGGER.warn("[MQ-CONSUMER] middleware=RabbitMQ queue={} messageId={} businessId={} x-death={} result=DLQ_OBSERVED durationMs=0",
                DLQ, raw.getMessageProperties().getMessageId(), raw.getMessageProperties().getHeader("businessId"),
                raw.getMessageProperties().getHeader("x-death"));
    }

    /**
     * 按来源队列隔离日志业务与幂等身份，并统一记录处理耗时及异常。
     * 失败必须重新抛出，让外层 MANUAL 方法选择拒绝，或让 AUTO 容器进行有限重试。
     */
    private String handle(String queue, MessageEvent event) {
        long start = System.nanoTime();
        try {
            String result = business.process("rabbit:" + queue, event);
            LOGGER.info("[MQ-CONSUMER] middleware=RabbitMQ queue={} messageId={} businessId={} result={} durationMs={}",
                    queue, event.messageId(), event.businessId(), result, (System.nanoTime() - start) / 1_000_000);
            return result;
        } catch (RuntimeException exception) {
            LOGGER.error("[MQ-CONSUMER] middleware=RabbitMQ queue={} messageId={} businessId={} result=FAILED durationMs={}",
                    queue, event.messageId(), event.businessId(), (System.nanoTime() - start) / 1_000_000, exception);
            throw exception;
        }
    }
}
