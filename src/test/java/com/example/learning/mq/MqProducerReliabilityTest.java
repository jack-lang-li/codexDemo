package com.example.learning.mq;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.rabbitmq.producer.RabbitMessageProducer;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 发布确认的反例：Confirm ACK+Return 仍应失败，DLT 写入失败不能假装恢复成功。 */
class MqProducerReliabilityTest {
    private final MessageEvent event = new MessageEvent("m-1", "O-1", "order.created", "test", Instant.now(), 0, null);

    @Test
    void rabbitConfirmAckWithoutRouteIsStillFailure() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(4);
            correlation.setReturned(new ReturnedMessage(new Message(new byte[0], new MessageProperties()),
                    312, "NO_ROUTE", "exchange", "bad-key"));
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(template).convertAndSend(anyString(), anyString(), eq(event), any(MessagePostProcessor.class), any(CorrelationData.class));
        assertThrows(IllegalStateException.class, () -> new RabbitMessageProducer(template, 50).send("exchange", "bad-key", event));
    }

    @Test
    void rabbitMissingConfirmationCannotReturnSuccessfulSend() {
        RabbitTemplate template = mock(RabbitTemplate.class);
        assertThrows(IllegalStateException.class, () -> new RabbitMessageProducer(template, 1).send("exchange", "key", event));
    }

}
