package com.example.learning.mq.common.service;

import com.example.learning.domain.order.OrderService;
import com.example.learning.domain.product.Product;
import com.example.learning.mq.common.dto.DemoRequest;
import com.example.learning.mq.common.dto.MessageDemoDTO;
import com.example.learning.mq.common.dto.OrderCreatedEvent;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * 模拟订单完成后产生事件，复用原有领域规则，绝不修改原 OrderService。
 * 此处订单是内存快照；生产中“数据库提交成功但发消息失败”需要 Outbox/补偿任务解决，
 * 单靠 Confirm 或 Kafka acks 无法把数据库事务和消息发送变成同一个原子操作。
 */
@Service
public class DemoEventService {
    private final OrderService orders = new OrderService(List.of(
            new Product("P-1001", "Notebook", new BigDecimal("19.90"))));

    public MessageDemoDTO task(DemoRequest request, String eventType) {
        return new MessageDemoDTO(UUID.randomUUID().toString(), request.businessId(), eventType,
                request.content(), Instant.now(), 0, null);
    }

    public MessageDemoDTO order(DemoRequest request, String eventType) {
        var order = orders.createOrder(request.businessId(),
                request.userId() == null || request.userId().isBlank() ? "U-DEMO" : request.userId(),
                "P-1001", request.quantity() == null ? 1 : request.quantity());
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();
        var event = new OrderCreatedEvent(id, order.getOrderId(), order.getBuyerId(),
                order.getTotalAmount(), now, eventType);
        return new MessageDemoDTO(id, order.getOrderId(), eventType, request.content(), now, 0, event);
    }
}
