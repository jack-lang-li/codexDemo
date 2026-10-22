package com.example.learning.application.order;

import com.example.learning.domain.order.Order;
import com.example.learning.domain.order.OrderService;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.common.dto.OrderCreatedEvent;
import com.example.learning.mq.kafka.constant.KafkaNames;
import com.example.learning.mq.kafka.producer.KafkaMessageProducer;
import com.example.learning.mq.rabbitmq.constant.RabbitMqNames;
import com.example.learning.mq.rabbitmq.producer.RabbitMessageProducer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * 订单应用层：原领域服务仍只负责订单规则，MQ 是同一服务内的后续事件发布能力。
 * 后续 Controller/其他业务可以直接注入本服务，不必调用任何 /demo 接口或启动另一应用。
 *
 * 当前订单仍是内存模型；接入数据库后，提交订单与发布消息之间需用 Outbox/补偿衔接。
 * 两种 MQ 独立确认，不是跨 Broker 事务；任一发布失败都向调用方报告，不能宣称原子完成。
 */
@Service
public class OrderApplicationService {
    private final OrderService orders;
    private final ObjectProvider<RabbitMessageProducer> rabbit;
    private final ObjectProvider<KafkaMessageProducer> kafka;

    public OrderApplicationService(OrderService orders, ObjectProvider<RabbitMessageProducer> rabbit,
            ObjectProvider<KafkaMessageProducer> kafka) {
        this.orders = orders;
        this.rabbit = rabbit;
        this.kafka = kafka;
    }

    public Order createOrder(String orderId, String buyerId, String productId, int quantity) {
        Order order = orders.createOrder(orderId, buyerId, productId, quantity);
        publishOrderCreated(order);
        return order;
    }

    private void publishOrderCreated(Order order) {
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();
        var payload = new OrderCreatedEvent(id, order.getOrderId(), order.getBuyerId(),
                order.getTotalAmount(), now, "order.created");
        var event = new MessageEvent(id, order.getOrderId(), "order.created", "订单创建成功", now, 0, payload);
        // 未启用的中间件不注册 Producer，原订单能力仍可使用；启用后等待各自 Broker 确认。
        RabbitMessageProducer rabbitProducer = rabbit.getIfAvailable();
        if (rabbitProducer != null) {
            rabbitProducer.send(RabbitMqNames.DIRECT, RabbitMqNames.CREATED_KEY, event);
        }
        KafkaMessageProducer kafkaProducer = kafka.getIfAvailable();
        if (kafkaProducer != null) {
            kafkaProducer.send(KafkaNames.ORDER, event).join();
        }
    }
}
