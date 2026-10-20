package com.example.learning.mq;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.teaching.mq.common.service.DemoBusinessService;
import com.example.learning.teaching.mq.kafka.consumer.KafkaDemoConsumer;
import com.example.learning.teaching.mq.rabbitmq.consumer.RabbitDemoConsumer;
import com.rabbitmq.client.Channel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/** ACK 必须发生在业务成功之后；异常不能推进消费位置，也不能误确认其他消息。 */
class MqConsumerAcknowledgmentTest {
    private final MessageEvent event = new MessageEvent("m-1", "O-1", "order.created", "test", Instant.now(), 0, null);
    private final DemoBusinessService business = mock(DemoBusinessService.class);

    @Test
    void rabbitSuccessAcknowledgesOnlyCurrentDeliveryAfterBusiness() throws Exception {
        Channel channel = mock(Channel.class);
        new RabbitDemoConsumer(business).manual(event, rabbitMessage(), channel);
        var sequence = inOrder(business, channel);
        sequence.verify(business).process("rabbit:order.created.queue", event);
        sequence.verify(channel).basicAck(42L, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void rabbitBusinessFailureNacksWithoutInfiniteRequeue() throws Exception {
        Channel channel = mock(Channel.class);
        when(business.process(anyString(), eq(event))).thenThrow(new IllegalStateException("failed"));
        new RabbitDemoConsumer(business).manual(event, rabbitMessage(), channel);
        verify(channel).basicNack(42L, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void rabbitInvalidBusinessRejectsWithoutRequeue() throws Exception {
        Channel channel = mock(Channel.class);
        when(business.process(anyString(), eq(event))).thenThrow(new IllegalArgumentException("invalid event"));
        new RabbitDemoConsumer(business).manual(event, rabbitMessage(), channel);
        verify(channel).basicReject(42L, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    void rabbitAckIoFailureDoesNotAttemptSecondConfirmation() throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new java.io.IOException("channel closed")).when(channel).basicAck(42L, false);
        assertThrows(java.io.IOException.class, () -> new RabbitDemoConsumer(business).manual(event, rabbitMessage(), channel));
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void kafkaCommitsOnlyAfterSuccessfulBusiness() {
        Acknowledgment ack = mock(Acknowledgment.class);
        var record = new ConsumerRecord<>("order-event-topic", 1, 42L, "O-1", event);
        new KafkaDemoConsumer(business).order(record, ack);
        var sequence = inOrder(business, ack);
        sequence.verify(business).process("kafka:order-service-group:order-event-topic", event);
        sequence.verify(ack).acknowledge();
    }

    @Test
    void kafkaFailurePropagatesWithoutCommitting() {
        Acknowledgment ack = mock(Acknowledgment.class);
        when(business.process(anyString(), eq(event))).thenThrow(new IllegalStateException("failed"));
        var record = new ConsumerRecord<>("order-event-topic", 1, 42L, "O-1", event);
        assertThrows(IllegalStateException.class, () -> new KafkaDemoConsumer(business).order(record, ack));
        verifyNoInteractions(ack);
    }

    private Message rabbitMessage() {
        var properties = new MessageProperties();
        properties.setDeliveryTag(42L);
        return new Message(new byte[0], properties);
    }
}
