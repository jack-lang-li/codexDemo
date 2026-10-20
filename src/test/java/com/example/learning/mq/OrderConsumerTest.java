package com.example.learning.mq;

import com.example.learning.application.order.OrderEventHandler;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.kafka.consumer.KafkaOrderConsumer;
import com.example.learning.mq.rabbitmq.consumer.RabbitOrderConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/** 验证主服务消费者的确认与异常传播，教学手动 ACK 另见 MqConsumerAcknowledgmentTest。 */
class OrderConsumerTest {
    private final OrderEventHandler handler = mock(OrderEventHandler.class);
    private final MessageEvent event = new MessageEvent("m-1", "O-1", "order.created", "test", Instant.now(), 0, null);

    @Test
    void kafkaAcknowledgesAfterBusiness() {
        Acknowledgment ack = mock(Acknowledgment.class);
        new KafkaOrderConsumer(handler).onOrderCreated(new ConsumerRecord<>("order-event-topic", 0, 1L, "O-1", event), ack);
        var sequence = inOrder(handler, ack);
        sequence.verify(handler).onOrderCreated("kafka:order-event-processing", event);
        sequence.verify(ack).acknowledge();
    }

    @Test
    void kafkaFailureDoesNotAcknowledge() {
        Acknowledgment ack = mock(Acknowledgment.class);
        when(handler.onOrderCreated(anyString(), eq(event))).thenThrow(new IllegalStateException("failed"));
        assertThrows(IllegalStateException.class, () -> new KafkaOrderConsumer(handler)
                .onOrderCreated(new ConsumerRecord<>("order-event-topic", 0, 1L, "O-1", event), ack));
        verifyNoInteractions(ack);
    }

    @Test
    void rabbitFailurePropagatesToContainerForRetryInsteadOfBeingSwallowed() {
        when(handler.onOrderCreated(anyString(), eq(event))).thenThrow(new IllegalStateException("failed"));
        assertThrows(IllegalStateException.class, () -> new RabbitOrderConsumer(handler).onOrderCreated(event));
    }
}
