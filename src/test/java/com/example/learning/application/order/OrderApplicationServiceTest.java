package com.example.learning.application.order;

import com.example.learning.domain.order.OrderService;
import com.example.learning.domain.product.Product;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.kafka.producer.KafkaMessageProducer;
import com.example.learning.mq.rabbitmq.producer.RabbitMessageProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 直接从实际订单函数观察 MQ 接入，不借助教学 Controller，不启动 Broker。 */
class OrderApplicationServiceTest {
    private final OrderService orders = new OrderService(List.of(new Product("P-1001", "Notebook", new BigDecimal("19.90"))));

    @Test
    @SuppressWarnings("unchecked")
    void orderFunctionPublishesSameStructuredEventToEnabledBrokers() {
        ObjectProvider<RabbitMessageProducer> rabbitProvider = mock(ObjectProvider.class);
        ObjectProvider<KafkaMessageProducer> kafkaProvider = mock(ObjectProvider.class);
        RabbitMessageProducer rabbit = mock(RabbitMessageProducer.class);
        KafkaMessageProducer kafka = mock(KafkaMessageProducer.class);
        when(rabbitProvider.getIfAvailable()).thenReturn(rabbit);
        when(kafkaProvider.getIfAvailable()).thenReturn(kafka);
        when(kafka.send(eq("order-event-topic"), any())).thenAnswer(invocation ->
                CompletableFuture.completedFuture(invocation.getArgument(1, MessageEvent.class)));
        var service = new OrderApplicationService(orders, rabbitProvider, kafkaProvider);

        var order = service.createOrder("O-1", "U-1", "P-1001", 2);

        assertThat(order.getTotalAmount()).isEqualByComparingTo("39.80");
        var captor = org.mockito.ArgumentCaptor.forClass(MessageEvent.class);
        verify(rabbit).send(eq("order.direct.exchange"), eq("order.created"), captor.capture());
        MessageEvent event = captor.getValue();
        verify(kafka).send("order-event-topic", event);
        assertThat(event.order().orderId()).isEqualTo(order.getOrderId());
        assertThat(event.order().eventId()).isEqualTo(event.messageId());
        assertThat(event.order().amount()).isEqualByComparingTo("39.80");
    }

    @Test
    @SuppressWarnings("unchecked")
    void disabledMqDoesNotPreventExistingOrderBusiness() {
        ObjectProvider<RabbitMessageProducer> rabbit = mock(ObjectProvider.class);
        ObjectProvider<KafkaMessageProducer> kafka = mock(ObjectProvider.class);
        var service = new OrderApplicationService(orders, rabbit, kafka);
        assertThat(service.createOrder("O-2", "U-1", "P-1001", 1).getTotalAmount()).isEqualByComparingTo("19.90");
    }

    @Test
    @SuppressWarnings("unchecked")
    void invalidOrderCannotPublishMessages() {
        ObjectProvider<RabbitMessageProducer> rabbit = mock(ObjectProvider.class);
        ObjectProvider<KafkaMessageProducer> kafka = mock(ObjectProvider.class);
        var service = new OrderApplicationService(orders, rabbit, kafka);
        assertThrows(IllegalArgumentException.class, () -> service.createOrder("O-3", "U-1", "P-1001", 0));
        verifyNoInteractions(rabbit, kafka);
    }

    @Test
    @SuppressWarnings("unchecked")
    void brokerFailureIsReportedWithoutClaimingCrossBrokerAtomicSuccess() {
        ObjectProvider<RabbitMessageProducer> rabbit = mock(ObjectProvider.class);
        ObjectProvider<KafkaMessageProducer> kafka = mock(ObjectProvider.class);
        KafkaMessageProducer producer = mock(KafkaMessageProducer.class);
        when(kafka.getIfAvailable()).thenReturn(producer);
        when(producer.send(anyString(), any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        var service = new OrderApplicationService(orders, rabbit, kafka);
        assertThrows(java.util.concurrent.CompletionException.class, () -> service.createOrder("O-4", "U-1", "P-1001", 1));
    }
}
