package com.example.learning.teaching.mq;

import com.example.learning.application.LearningApplication;
import com.example.learning.teaching.mq.config.MqTeachingConfiguration;
import org.springframework.context.annotation.Import;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.teaching.mq.common.service.DemoEventService;
import com.example.learning.teaching.mq.common.dto.DemoRequest;
import com.example.learning.mq.kafka.producer.KafkaMessageProducer;
import com.example.learning.mq.rabbitmq.producer.RabbitMessageProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * 装配LearningApplication + 显式教学配置，但禁止 Rabbit Listener/Admin 与 Kafka Listener/Admin 发起连接。
 * 不启动内嵌 Broker、不连接占位地址；验证 Bean、MVC、安全放行、文档和 JSON 协议。
 */
@SpringBootTest(classes = LearningApplication.class, properties = {
        "app.mq.rabbitmq.enabled=true", "app.mq.kafka.enabled=true", "app.mq.order-listeners-enabled=false",
        "spring.rabbitmq.listener.simple.auto-startup=false", "spring.rabbitmq.dynamic=false",
        "spring.kafka.listener.auto-startup=false", "spring.kafka.admin.auto-create=false"})
@Import(MqTeachingConfiguration.class)
@AutoConfigureMockMvc
class MqTeachingTest {
    @Autowired MockMvc mvc;
    @Autowired ObjectMapper mapper;
    @Autowired DemoEventService events;
    @Autowired Jackson2JsonMessageConverter rabbitConverter;
    @Autowired Declarables rabbitDemoTopology;
    @Autowired ProducerFactory<Object, Object> producerFactory;
    @Autowired ConsumerFactory<Object, Object> consumerFactory;
    @Autowired RabbitListenerEndpointRegistry rabbitListeners;
    @Autowired KafkaListenerEndpointRegistry kafkaListeners;
    @MockitoBean RabbitMessageProducer rabbit;
    @MockitoBean KafkaMessageProducer kafka;

    @Test
    void orderSnapshotAndJsonRoundTripPreserveMoneyTimeAndIdentity() throws Exception {
        var event = events.order(new DemoRequest("O-1", "test", null, "U-1", 2), "order.created");
        assertThat(event.order().amount()).isEqualByComparingTo(new BigDecimal("39.80"));
        assertThat(event.order().eventId()).isEqualTo(event.messageId());
        assertThat(mapper.readValue(mapper.writeValueAsBytes(event), MessageEvent.class)).isEqualTo(event);
        var properties = new org.springframework.amqp.core.MessageProperties();
        assertThat(rabbitConverter.fromMessage(rabbitConverter.toMessage(event, properties))).isEqualTo(event);
    }

    @Test
    void declaresAllBusinessQueuesWithDlx() {
        var queues = rabbitDemoTopology.getDeclarablesByType(Queue.class);
        assertThat(queues).hasSize(7);
        assertThat(queues).filteredOn(queue -> !queue.getName().equals("order.process.dlq"))
                .allSatisfy(queue -> assertThat(queue.getArguments()).containsEntry("x-dead-letter-exchange", "order.process.dlx"));
    }

    @Test
    void actualKafkaSerializersRoundTripDtoAndPreserveMalformedDltBytes() {
        var event = events.order(new DemoRequest("O-1", "test", null, "U-1", 2), "order.created");
        var serializer = producerFactory.getValueSerializer();
        byte[] json = serializer.serialize("order-event-topic", event);
        try (var deserializer = new ErrorHandlingDeserializer<MessageEvent>()) {
            deserializer.configure(consumerFactory.getConfigurationProperties(), false);
            assertThat(deserializer.deserialize("order-event-topic", new org.apache.kafka.common.header.internals.RecordHeaders(), json))
                    .isEqualTo(event);
            byte[] malformed = "not-json".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
            assertThat(deserializer.deserialize("order-event-topic", headers, malformed)).isNull();
            assertThat(headers.toArray()).isNotEmpty();
            assertThat(serializer.serialize("order-event-topic.DLT", malformed)).isEqualTo(malformed);
        }
    }

    @Test
    void noRabbitOrKafkaListenerStartsDuringOfflineTests() {
        assertThat(rabbitListeners.getListenerContainers()).isNotEmpty().allSatisfy(container -> assertThat(container.isRunning()).isFalse());
        assertThat(kafkaListeners.getListenerContainers()).isNotEmpty().allSatisfy(container -> assertThat(container.isRunning()).isFalse());
    }

    @Test
    void rabbitDuplicateEndpointPublishesSameMessageIdTwice() throws Exception {
        mvc.perform(post("/demo/rabbitmq/idempotent").contentType("application/json")
                        .content("{\"businessId\":\"O-1\",\"content\":\"test\"}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("messageId").isNotEmpty());
        var captor = org.mockito.ArgumentCaptor.forClass(MessageEvent.class);
        verify(rabbit, times(2)).send(eq("order.direct.exchange"), eq("order.created"), captor.capture());
        assertThat(captor.getAllValues().get(0)).isSameAs(captor.getAllValues().get(1));
    }

    @Test
    void kafkaApiWaitsForBrokerFutureAndReturnsStructuredEvent() throws Exception {
        when(kafka.send(eq("order-event-topic"), any())).thenAnswer(invocation ->
                CompletableFuture.completedFuture(invocation.getArgument(1, MessageEvent.class)));
        var result = mvc.perform(post("/demo/kafka/order").contentType("application/json")
                        .content("{\"businessId\":\"O-2\",\"content\":\"test\"}"))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(result)).andExpect(status().isAccepted())
                .andExpect(jsonPath("order.orderId").value("O-2"));
    }

    @Test
    void invalidInputIs400AndSwaggerListsMqEndpoints() throws Exception {
        mvc.perform(post("/demo/rabbitmq/simple").contentType("application/json").content("{}"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/v3/api-docs")).andExpect(status().isOk())
                .andExpect(jsonPath("paths['/demo/rabbitmq/error']").exists())
                .andExpect(jsonPath("paths['/demo/kafka/idempotent']").exists());
    }
}
