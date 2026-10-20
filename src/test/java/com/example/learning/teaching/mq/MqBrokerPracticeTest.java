package com.example.learning.teaching.mq;

import com.example.learning.application.LearningApplication;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.teaching.mq.common.dto.DemoRequest;
import com.example.learning.teaching.mq.common.service.DemoBusinessService;
import com.example.learning.teaching.mq.config.MqTeachingConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

/**
 * 手动实战：配置自己的 RabbitMQ/Kafka/Redis 后，设置 MQ_PRACTICE=true 再运行本测试。
 * 默认跳过，不启动任何 Broker；主类仍是 LearningApplication，没有第二个启动服务。
 * 教学端点只通过 MockMvc 调用；消费者、网络与 Redis 均真实运行，请使用独立学习环境。
 */
@EnabledIfEnvironmentVariable(named = "MQ_PRACTICE", matches = "true")
@SpringBootTest(classes = LearningApplication.class, properties = {
        "app.mq.rabbitmq.enabled=true", "app.mq.kafka.enabled=true", "app.mq.order-listeners-enabled=false"})
@Import(MqTeachingConfiguration.class)
@AutoConfigureMockMvc
class MqBrokerPracticeTest {
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @MockitoSpyBean DemoBusinessService business;

    @Test
    void rabbitQueueDirectTopicAndFanout() throws Exception {
        MessageEvent simple = send("rabbitmq/simple", null);
        observed("rabbit:demo.task.queue", simple, 1);
        MessageEvent direct = send("rabbitmq/direct", null);
        observed("rabbit:order.created.queue", direct, 1);
        MessageEvent topic = send("rabbitmq/topic", "order.paid");
        observed("rabbit:order.topic.star.queue", topic, 1);
        observed("rabbit:order.topic.hash.queue", topic, 1);
        MessageEvent deepTopic = send("rabbitmq/topic", "order.payment.completed");
        observed("rabbit:order.topic.hash.queue", deepTopic, 1);
        MessageEvent fanout = send("rabbitmq/fanout", null);
        for (String channel : new String[]{"email", "sms", "audit"}) {
            observed("rabbit:user.register." + channel + ".queue", fanout, 1);
        }
    }

    @Test
    void rabbitDuplicateAndBoundedRetry() throws Exception {
        MessageEvent duplicate = send("rabbitmq/idempotent", null);
        observed("rabbit:order.created.queue", duplicate, 2);
        // 日志中同一 messageId 一次 SUCCESS、一次 DUPLICATE_SKIPPED。
        MessageEvent failure = send("rabbitmq/error", null);
        observed("rabbit:order.process.queue", failure, 3);
        // 在管理台观察 order.process.dlq；默认不启用死信观察器，保留失败证据。
    }

    @Test
    void kafkaPartitionGroupsAndDuplicateDelivery() throws Exception {
        MessageEvent order = send("kafka/order", null);
        observed("kafka:order-service-group:order-event-topic", order, 1);
        observed("kafka:analytics-service-group:order-event-topic", order, 1);
        MessageEvent duplicate = send("kafka/idempotent", null);
        observed("kafka:order-service-group:order-event-topic", duplicate, 2);
        observed("kafka:analytics-service-group:order-event-topic", duplicate, 2);
    }

    @Test
    void kafkaRetriesBeforeDlt() throws Exception {
        MessageEvent failure = send("kafka/error", null);
        observed("kafka:order-service-group:order-event-topic", failure, 3);
        observed("kafka:analytics-service-group:order-event-topic", failure, 3);
        // 两个组分别恢复，通常会产生两条 DLT；通过原 topic/partition/offset 与异常头追踪。
    }

    private MessageEvent send(String endpoint, String eventType) throws Exception {
        var request = new DemoRequest("O-PRACTICE-" + UUID.randomUUID(), "学习事件", eventType, "U-PRACTICE", 1);
        var result = mvc.perform(post("/demo/" + endpoint).contentType("application/json")
                .content(mapper.writeValueAsBytes(request))).andReturn();
        if (result.getRequest().isAsyncStarted()) {
            result = mvc.perform(asyncDispatch(result)).andReturn();
        }
        assertThat(result.getResponse().getStatus()).isEqualTo(202);
        return mapper.readValue(result.getResponse().getContentAsByteArray(), MessageEvent.class);
    }

    private void observed(String subscriber, MessageEvent event, int deliveries) {
        verify(business, timeout(15000).times(deliveries)).process(eq(subscriber),
                argThat(received -> received.messageId().equals(event.messageId())));
    }
}
