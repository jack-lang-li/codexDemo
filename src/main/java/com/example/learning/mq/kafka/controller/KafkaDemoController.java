package com.example.learning.mq.kafka.controller;

import com.example.learning.mq.common.dto.DemoRequest;
import com.example.learning.mq.common.dto.MessageDemoDTO;
import com.example.learning.mq.common.service.DemoEventService;
import com.example.learning.mq.kafka.producer.KafkaDemoProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

import static com.example.learning.mq.kafka.constant.KafkaNames.*;

/** HTTP 异步响应等待 Producer 的 Broker ACK 后返回 202；消费成功请观察消费日志和业务记录。 */
@RestController
@RequestMapping("/demo/kafka")
@Tag(name = "Kafka 学习", description = "分区、消费组、Offset、重试、DLT 与幂等")
public class KafkaDemoController {
    private final DemoEventService events;
    private final KafkaDemoProducer producer;

    public KafkaDemoController(DemoEventService events, KafkaDemoProducer producer) {
        this.events = events;
        this.producer = producer;
    }

    @PostMapping("/simple")
    @Operation(summary = "发送结构化任务到 demo-topic，观察分区/offset/key/时间")
    public CompletableFuture<ResponseEntity<MessageDemoDTO>> simple(@Valid @RequestBody DemoRequest request) {
        return producer.send(SIMPLE, events.task(request, "task.submitted"))
                .thenApply(event -> ResponseEntity.accepted().body(event));
    }

    @PostMapping("/order")
    @Operation(summary = "订单 ID 作为 key；订单组和统计组分别消费")
    public CompletableFuture<ResponseEntity<MessageDemoDTO>> order(@Valid @RequestBody DemoRequest request) {
        return producer.send(ORDER, events.order(request, "order.created")).thenApply(event -> ResponseEntity.accepted().body(event));
    }

    @PostMapping("/error")
    @Operation(summary = "故意失败，首次+两次重试后发布 order-event-topic.DLT")
    public CompletableFuture<ResponseEntity<MessageDemoDTO>> error(@Valid @RequestBody DemoRequest request) {
        return producer.send(ORDER, events.order(request, "demo.error")).thenApply(event -> ResponseEntity.accepted().body(event));
    }

    @PostMapping("/idempotent")
    @Operation(summary = "同一 messageId 显式发送两次，各消费组内只处理一次")
    public CompletableFuture<ResponseEntity<MessageDemoDTO>> idempotent(@Valid @RequestBody DemoRequest request) {
        MessageDemoDTO event = events.order(request, "order.created");
        return producer.send(ORDER, event).thenCompose(ignored -> producer.send(ORDER, event))
                .thenApply(sent -> ResponseEntity.accepted().body(sent));
    }
}
