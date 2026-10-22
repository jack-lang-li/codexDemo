package com.example.learning.teaching.mq.manual;

import com.example.learning.mq.common.dto.MessageEvent;
import com.example.learning.mq.rabbitmq.producer.RabbitMessageProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.ssl.SslAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IDEA 中分别运行两个 @Test 方法的 RabbitMQ 收发实验，无需启动 LearningApplication 或创建订单。
 * 先运行 consumeMessages 持续监听，再在另一个运行窗口点 sendMessage；反过来也可以，队列会暂存消息。
 * 仅加载 RabbitMQ 与 JSON 配置，读取原 application.yml 的连接参数，不装配订单 Service、Kafka 或 Redis。
 * 类名以 Demo 结尾，常规 mvn test 不会自动运行这个持续监听实验；IDEA 可直接点击方法旁绿色箭头。
 */
@SpringBootTest(classes = RabbitMqProducerConsumerDemo.DemoConfiguration.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "app.mq.rabbitmq.enabled=true", "spring.rabbitmq.dynamic=true",
        "spring.rabbitmq.template.mandatory=true", "spring.rabbitmq.listener.simple.concurrency=1",
        "spring.rabbitmq.listener.simple.max-concurrency=1"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RabbitMqProducerConsumerDemo {
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqProducerConsumerDemo.class);
    private static final String QUEUE = "learning.idea.rabbit.messages.queue";
    private static final String LISTENER = "ideaRabbitConsumer";

    @Autowired RabbitMessageProducer producer;
    @Autowired RabbitListenerEndpointRegistry registry;
    @Autowired DemoConfiguration configuration;

    // IDEA 默认 0：一直监听，点击 Stop 结束。命令行验证可设正数：收到一条消息即结束，超时则失败。
    @Value("${mq.demo.receive-timeout-seconds:0}")
    long receiveTimeoutSeconds;

    /**
     * 生产者入口：在 IDEA 点击此方法的绿色箭头，构造一条消息并真正发送到 YAML 中的 RabbitMQ。
     * 空交换机名代表默认交换机，以队列名精确路由；本进程的消费者默认不启动，不会自己抢走消息。
     * 返回表示已获 Broker Confirm 且没有 Return，随后测试结束；另一个进程中的消费者独立接收。
     * content 可以直接修改，重复运行会生成新的 messageId，方便在两个控制台中逐条对照。
     */
    @Test
    void sendMessage() {
        var event = new MessageEvent(UUID.randomUUID().toString(), "IDEA-RABBIT-DEMO",
                "demo.message", "你好 RabbitMQ，这是在 IDEA 点击测试发送的真实消息", Instant.now(), 0, null);
        producer.send("", QUEUE, event);
        LOGGER.info("[IDEA-RABBIT-SENT] queue={} messageId={} content={}", QUEUE, event.messageId(), event.content());
    }

    /**
     * 消费者入口：在 IDEA 单独运行此方法，开启监听器后持续等待，收到消息时由 receive 方法打印内容。
     * 此方法不发送消息、不调用任何业务 Service；保持窗口运行，再单独运行 sendMessage 即可观察收发。
     * 默认等待直到手动 Stop；receive-timeout-seconds 正数仅用于自动核验首次真实收消息，超时断言失败。
     * finally 停止监听容器，配合 DirtiesContext 关闭真实连接，避免测试结束后还占用消费者。
     * @throws InterruptedException 等待线程被中断时向测试框架传播
     */
    @Test
    void consumeMessages() throws InterruptedException {
        var container = registry.getListenerContainer(LISTENER);
        assertThat(container).as("已注册独立 RabbitMQ 消费者").isNotNull();
        try {
            container.start();
            LOGGER.info("[IDEA-RABBIT-LISTENING] queue={}，保持本测试运行；在另一个窗口运行 sendMessage，点击 Stop 结束", QUEUE);
            if (receiveTimeoutSeconds > 0) {
                assertThat(configuration.firstMessage.await(receiveTimeoutSeconds, TimeUnit.SECONDS))
                        .as("限定时间内从真实 RabbitMQ 收到消息").isTrue();
            } else {
                new CountDownLatch(1).await();
            }
        } finally {
            container.stop();
        }
    }

    /**
     * 本文件独立的测试装配：只导入 RabbitMQ、SSL 参数支持、JSON 自动配置和已有真实生产者。
     * 不使用组件扫描，不加载 LearningApplication；生产者、消费者两个独立测试进程共用同一个队列名。
     * 没有 main 方法，不是新增应用启动类；firstMessage 只帮助限时核验等待实际回调，不代替 MQ。
     */
    @Configuration(proxyBeanMethods = false)
    @EnableRabbit
    @ImportAutoConfiguration({SslAutoConfiguration.class, JacksonAutoConfiguration.class, RabbitAutoConfiguration.class})
    @Import(RabbitMessageProducer.class)
    static class DemoConfiguration {
        private final CountDownLatch firstMessage = new CountDownLatch(1);

        /**
         * 在真实服务器声明独立的持久队列，让先发送、后启动消费者也能收到积压消息。
         * 使用专门的 learning.idea 名称，与正式订单及原自动测试队列隔离；不删除队列或清空已有消息。
         */
        @Bean
        Queue ideaRabbitQueue() {
            return QueueBuilder.durable(QUEUE).build();
        }

        /**
         * 让真实 RabbitTemplate 与监听容器用同一个 JSON 协议传输 MessageEvent，包括 Instant 时间字段。
         * 复用 Boot 的 ObjectMapper，并限定可反序列化 DTO 包；消息经网络传输而非直接调用接收方法。
         */
        @Bean
        Jackson2JsonMessageConverter ideaRabbitJsonConverter(ObjectMapper mapper) {
            return new Jackson2JsonMessageConverter(mapper, "com.example.learning.mq.common.dto");
        }

        /**
         * 真正的消费者回调：RabbitMQ 推送消息后由 Spring 调用，打印发送端填写的内容及消息编号。
         * autoStartup=false 防止生产者测试顺便启动消费者；只有 consumeMessages 显式启动它。
         * acknowledgeMode=AUTO 表示本方法正常返回后由 Spring ACK；实际业务应在返回前完成，失败应抛异常。
         */
        @RabbitListener(id = LISTENER, queues = QUEUE, autoStartup = "false", ackMode = "AUTO")
        void receive(MessageEvent event) {
            LOGGER.info("[IDEA-RABBIT-RECEIVED] queue={} messageId={} businessId={} content={} createTime={}",
                    QUEUE, event.messageId(), event.businessId(), event.content(), event.createTime());
            firstMessage.countDown();
        }
    }
}
