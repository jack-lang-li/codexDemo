package com.example.learning.teaching.mq.config;

import com.example.learning.teaching.mq.common.service.DemoBusinessService;
import com.example.learning.teaching.mq.common.service.MqConsumptionProbe;
import com.example.learning.teaching.mq.kafka.consumer.KafkaDemoConsumer;
import com.example.learning.teaching.mq.rabbitmq.consumer.RabbitDemoConsumer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * 教学内容只由测试显式 @Import；不提供第二个 main，不进入正常服务的扫描范围或打包制品。
 * 测试直接调用真实生产者，不使用 MockMvc/Mockito；消费者真实连接 YAML 中的服务器。
 */
@TestConfiguration(proxyBeanMethods = false)
@Import({RabbitTeachingConfiguration.class, DemoBusinessService.class,
        RabbitDemoConsumer.class, KafkaDemoConsumer.class})
public class MqTeachingConfiguration {
    /**
     * 注册一个只活在测试容器中的结果记录器，供真实监听线程写入、测试线程等待和断言。
     * 不改变生产者、客户端、确认机制或业务处理流程，不会进入正常服务制品。
     */
    @Bean
    MqConsumptionProbe consumptionProbe() {
        return new MqConsumptionProbe();
    }

    /**
     * 在启用 Kafka 的测试中声明独立练习 Topic 及 DLT，每个三个分区、一个副本。
     * 真实 KafkaAdmin 使用 YAML 账号创建资源；名称与正式订单 Topic 隔离，不能依赖本地假 Topic。
     */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "app.mq.kafka.enabled", havingValue = "true")
    KafkaAdmin.NewTopics teachingTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(KafkaTeachingNames.SIMPLE).partitions(3).replicas(1).build(),
                TopicBuilder.name(KafkaTeachingNames.SIMPLE + ".DLT").partitions(3).replicas(1).build(),
                TopicBuilder.name(KafkaTeachingNames.ORDER).partitions(3).replicas(1).build(),
                TopicBuilder.name(KafkaTeachingNames.ORDER + ".DLT").partitions(3).replicas(1).build());
    }
}
