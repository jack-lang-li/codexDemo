package com.example.learning.application;

import com.example.learning.application.order.OrderApplicationService;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.kafka.consumer.KafkaOrderConsumer;
import com.example.learning.mq.kafka.producer.KafkaMessageProducer;
import com.example.learning.mq.rabbitmq.consumer.RabbitOrderConsumer;
import com.example.learning.mq.rabbitmq.producer.RabbitMessageProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 统一服务装配：真实业务消费者存在，教学端点不存在；全部 Broker 网络操作关闭。 */
@SpringBootTest(classes = LearningApplication.class, properties = {
        "app.mq.rabbitmq.enabled=true", "app.mq.kafka.enabled=true",
        "spring.rabbitmq.dynamic=false", "spring.rabbitmq.listener.simple.auto-startup=false",
        "spring.kafka.admin.auto-create=false", "spring.kafka.listener.auto-startup=false"})
class LearningApplicationTest {
    @Autowired OrderApplicationService orders;
    @Autowired ApplicationContext context;
    @Autowired @Qualifier("requestMappingHandlerMapping") RequestMappingHandlerMapping mappings;
    @MockitoBean RabbitMessageProducer rabbit;
    @MockitoBean KafkaMessageProducer kafka;

    @Test
    void enabledMqIsWiredIntoExistingServiceBusiness() {
        when(kafka.send(anyString(), any())).thenAnswer(invocation ->
                CompletableFuture.completedFuture(invocation.getArgument(1, MessageEvent.class)));
        assertThat(orders.createOrder("O-SERVICE-1", "U-1", "P-1001", 2).getTotalAmount()).isEqualByComparingTo("39.80");
        verify(rabbit).send(eq("order.direct.exchange"), eq("order.created"), any(MessageEvent.class));
        verify(kafka).send(eq("order-event-topic"), any(MessageEvent.class));
        assertThat(context.getBeansOfType(RabbitOrderConsumer.class)).hasSize(1);
        assertThat(context.getBeansOfType(KafkaOrderConsumer.class)).hasSize(1);
    }

    @Test
    void normalServiceDoesNotExposeTeachingControllersOrFaultInjection() {
        assertThat(mappings.getHandlerMethods().keySet()).allSatisfy(mapping ->
                assertThat(mapping.getPatternValues()).noneMatch(path -> path.startsWith("/demo/")));
        assertThat(context.getBeanNamesForType(com.example.learning.teaching.mq.common.service.DemoBusinessService.class)).isEmpty();
        assertThat(context.containsBean("rabbitManualContainerFactory")).isFalse();
    }
}
