package com.example.learning.teaching.mq.common.service;

import com.example.learning.mq.common.dto.MessageEvent;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 被动记录真实消费者的结果，不替换生产者、消费者、Channel、Acknowledgment 或 Redis。
 * 测试必须先经网络发送，再等待 @RabbitListener/@KafkaListener 调用这里，不能直接调用监听方法。
 * 记录按随机 messageId 隔离，服务重启或其他客户端的旧消息不会误使断言通过。
 */
public class MqConsumptionProbe {
    /**
     * 一次真实监听器回调的只读结果：保存订阅者、消息及业务结果。
     * Kafka 还保存真实 partition、offset、key；RabbitMQ 没有这些概念，对应字段为 null。
     */
    public record Delivery(String subscriber, MessageEvent event, String result,
            Integer partition, Long offset, String key) {
    }

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Delivery>> deliveries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> executions = new ConcurrentHashMap<>();

    /**
     * 在真实业务入口每被调用一次时累加尝试次数，包含异常重试和重复投递。
     * 此计数不发送消息、不执行重试，重试仍由真实监听容器驱动。
     */
    public void attempted(String subscriber, MessageEvent event) {
        attempts.computeIfAbsent(key(subscriber, event.messageId()), ignored -> new AtomicInteger()).incrementAndGet();
    }

    /**
     * 仅在幂等保护内的业务正常完成时累加，区分“收到两次”和“业务执行两次”。
     * 重复消息跳过业务，失败消息抛出异常，二者都不应增加此计数。
     */
    public void businessExecuted(String subscriber, MessageEvent event) {
        executions.computeIfAbsent(key(subscriber, event.messageId()), ignored -> new AtomicInteger()).incrementAndGet();
    }

    /**
     * 由真实消费者记录已完成的业务结果；手动确认案例在 ACK/offset 提交调用成功后记录。
     * RabbitMQ AUTO 案例在监听方法返回前记录，此结果只证明业务完成，最终 ACK 由容器负责。
     */
    public void received(String subscriber, MessageEvent event, String result,
            Integer partition, Long offset, String messageKey) {
        deliveries.computeIfAbsent(key(subscriber, event.messageId()), ignored -> new CopyOnWriteArrayList<>())
                .add(new Delivery(subscriber, event, result, partition, offset, messageKey));
    }

    /**
     * 返回指定订阅者及消息的结果快照，尚未收到返回空列表。
     * 线程安全容器允许测试线程读取监听线程写入的结果，调用者不能修改内部记录。
     */
    public List<Delivery> deliveries(String subscriber, String messageId) {
        var result = deliveries.get(key(subscriber, messageId));
        return result == null ? List.of() : List.copyOf(result);
    }

    /**
     * 查询真实业务入口的累计调用次数；未发生回调时为零，用于核对有限重试次数。
     */
    public int attempts(String subscriber, String messageId) {
        var value = attempts.get(key(subscriber, messageId));
        return value == null ? 0 : value.get();
    }

    /**
     * 查询成功执行核心业务的累计次数，重复投递案例应为一，失败案例应为零。
     */
    public int executions(String subscriber, String messageId) {
        var value = executions.get(key(subscriber, messageId));
        return value == null ? 0 : value.get();
    }

    /**
     * 组合订阅者和 messageId，隔离不同队列、消费组及不同测试消息的观察结果。
     */
    private String key(String subscriber, String messageId) {
        return subscriber + "|" + messageId;
    }
}
