package com.example.learning.application;

import com.example.learning.application.order.OrderApplicationService;
import com.example.learning.mq.kafka.producer.KafkaMessageProducer;
import com.example.learning.mq.rabbitmq.producer.RabbitMessageProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/** 不指定 profile 或服务器参数，验证默认 application.yml 可装配且不连接占位 MQ。 */
@SpringBootTest(classes = LearningApplication.class)
class LearningApplicationDefaultsTest {
    @Autowired ApplicationContext context;
    @Autowired OrderApplicationService orders;

    @Test
    void defaultConfigurationKeepsOrderAvailableWithoutConfiguredMq() {
        assertThat(context.getEnvironment().getProperty("spring.application.name")).isEqualTo("java-learning-project");
        assertThat(context.getBeansOfType(RabbitMessageProducer.class)).isEmpty();
        assertThat(context.getBeansOfType(KafkaMessageProducer.class)).isEmpty();
        assertThat(context.getBeansOfType(com.example.learning.teaching.mq.config.MqTeachingConfiguration.class)).isEmpty();
        assertThat(orders.createOrder("O-DEFAULT", "U-1", "P-1001", 1).getTotalAmount()).isEqualByComparingTo("19.90");
    }
}
