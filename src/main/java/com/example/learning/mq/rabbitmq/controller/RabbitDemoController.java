package com.example.learning.mq.rabbitmq.controller;

import com.example.learning.mq.common.dto.DemoRequest;
import com.example.learning.mq.common.dto.MessageDemoDTO;
import com.example.learning.mq.common.service.DemoEventService;
import com.example.learning.mq.rabbitmq.producer.RabbitDemoProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

import static com.example.learning.mq.rabbitmq.constant.RabbitMqNames.*;

/** Controller -> 事件服务 -> Producer -> Broker -> Consumer -> 业务服务；202 只表示 Broker 确认发送。 */
@RestController
@RequestMapping("/demo/rabbitmq")
@Tag(name = "RabbitMQ 学习", description = "路由、确认、重试、死信、Redis 幂等")
public class RabbitDemoController {
    private final DemoEventService events;
    private final RabbitDemoProducer producer;

    public RabbitDemoController(DemoEventService events, RabbitDemoProducer producer) {
        this.events = events;
        this.producer = producer;
    }

    @PostMapping("/simple")
    @Operation(summary = "默认交换机：提交异步任务")
    public ResponseEntity<MessageDemoDTO> simple(@Valid @RequestBody DemoRequest request) {
        return publish("", SIMPLE, events.task(request, "task.submitted"));
    }

    @PostMapping("/direct")
    @Operation(summary = "Direct 精确路由订单事件；消费者 MANUAL ACK")
    public ResponseEntity<MessageDemoDTO> direct(@Valid @RequestBody DemoRequest request) {
        return publish(DIRECT, CREATED_KEY, events.order(request, "order.created"));
    }

    @PostMapping("/topic")
    @Operation(summary = "Topic 通配符：order.* 与 order.#")
    public ResponseEntity<MessageDemoDTO> topic(@Valid @RequestBody DemoRequest request) {
        String type = request.eventType() == null ? CREATED_KEY : request.eventType();
        if (!Set.of("order.created", "order.paid", "order.cancelled", "order.payment.completed").contains(type)) {
            throw new IllegalArgumentException("不支持的订单事件路由");
        }
        return publish(TOPIC, type, events.order(request, type));
    }

    @PostMapping("/fanout")
    @Operation(summary = "注册事件广播：邮件、短信、操作日志三个独立订阅")
    public ResponseEntity<MessageDemoDTO> fanout(@Valid @RequestBody DemoRequest request) {
        return publish(FANOUT, "", events.task(request, "user.registered"));
    }

    @PostMapping("/error")
    @Operation(summary = "固定失败：最多三次尝试后进入 order.process.dlq")
    public ResponseEntity<MessageDemoDTO> error(@Valid @RequestBody DemoRequest request) {
        return publish(DIRECT, PROCESS_KEY, events.order(request, "demo.error"));
    }

    @PostMapping("/idempotent")
    @Operation(summary = "连续发布两次同一 messageId，订单后续逻辑只处理一次")
    public ResponseEntity<MessageDemoDTO> idempotent(@Valid @RequestBody DemoRequest request) {
        MessageDemoDTO event = events.order(request, "order.created");
        producer.send(DIRECT, CREATED_KEY, event);
        return publish(DIRECT, CREATED_KEY, event);
    }

    private ResponseEntity<MessageDemoDTO> publish(String exchange, String key, MessageDemoDTO event) {
        producer.send(exchange, key, event);
        return ResponseEntity.accepted().body(event);
    }
}
