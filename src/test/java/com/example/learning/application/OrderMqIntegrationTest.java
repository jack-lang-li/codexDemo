package com.example.learning.application;

import com.example.learning.application.order.OrderApplicationService;
import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.kafka.constant.KafkaNames;
import com.example.learning.mq.rabbitmq.constant.RabbitMqNames;
import com.example.learning.teaching.mq.support.RealKafkaRecords;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 正式业务链路：调用订单函数，两个真实 Broker 收到同一事件，主代码的两个消费者完成 Redis 幂等。
 * 临时 Rabbit 观察队列和独立 Kafka 观察客户端均为真实客户端，不与业务消费者争抢消息。
 */
@SpringBootTest(classes = LearningApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "app.mq.rabbitmq.enabled=true", "app.mq.kafka.enabled=true", "app.mq.order-listeners-enabled=true",
        "spring.rabbitmq.dynamic=true", "spring.rabbitmq.listener.simple.auto-startup=true",
        "spring.kafka.admin.auto-create=true", "spring.kafka.admin.fail-fast=true", "spring.kafka.listener.auto-startup=true"})
class OrderMqIntegrationTest {
    @Autowired OrderApplicationService orders;
    @Autowired RabbitTemplate rabbit;
    @Autowired StringRedisTemplate redis;
    @Autowired KafkaProperties kafka;
    @Autowired SslBundles sslBundles;
    @Autowired ObjectMapper mapper;

    /**
     * 推荐先读的完整业务案例：调用订单应用服务创建两件商品的订单，由业务函数自动发布两个 MQ 事件。
     * 通过临时 RabbitMQ 绑定队列和独立 Kafka 客户端验证真实消息，再检查两个正式消费者的 Redis DONE。
     * 观察队列只复制消息，finally 只删除本次临时队列；不从业务队列抢消息，也不清理永久死信。
     */
    @Test
    void orderBusinessPublishesAndBothProductionConsumersReallyProcessIt() throws Exception {
        String copy = rabbit.execute(channel -> {
            String queue = channel.queueDeclare("", false, true, true, null).getQueue();
            channel.queueBind(queue, RabbitMqNames.DIRECT, RabbitMqNames.CREATED_KEY);
            return queue;
        });
        try (var kafkaRecords = new RealKafkaRecords(kafka, sslBundles, KafkaNames.ORDER)) {
            String orderId = "O-REAL-SERVICE-" + UUID.randomUUID();
            var order = orders.createOrder(orderId, "U-REAL-TEST", "P-1001", 2);
            assertThat(order.getTotalAmount()).isEqualByComparingTo("39.80");

            Object received = rabbit.receiveAndConvert(copy, 30000);
            assertThat(received).isInstanceOf(MessageEvent.class);
            MessageEvent event = (MessageEvent) received;
            assertThat(event.businessId()).isEqualTo(orderId);

            var record = kafkaRecords.awaitMessage(event.messageId(), 1).get(0);
            assertThat(record.key()).isEqualTo(orderId);
            assertThat(mapper.readValue(record.value(), MessageEvent.class)).isEqualTo(event);

            await().alias("主代码中的真实 RabbitMQ/Kafka 消费者完成订单业务")
                    .atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                        assertThat(redis.opsForValue().get("learning:mq:consumed:rabbit:order-followup:" + event.messageId())).isEqualTo("DONE");
                        assertThat(redis.opsForValue().get("learning:mq:consumed:kafka:order-event-processing:" + event.messageId())).isEqualTo("DONE");
                    });
        } finally {
            rabbit.execute(channel -> { channel.queueDelete(copy); return null; });
        }
    }
}
