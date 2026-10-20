package com.example.learning.mq.kafka.config;

import com.example.learning.mq.common.dto.MessageEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** 直接验证当前版本的真实 Recoverer，不需要运行 Kafka。 */
class KafkaRecoveryTest {
    private KafkaTemplate<Object, Object> template;
    private DeadLetterPublishingRecoverer recoverer;
    private final MessageEvent event = new MessageEvent("m-1", "O-1", "demo.error", "test", Instant.now(), 0, null);

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        template = mock(KafkaTemplate.class);
        ProducerFactory<Object, Object> factory = mock(ProducerFactory.class);
        when(template.getProducerFactory()).thenReturn(factory);
        when(factory.getConfigurationProperties()).thenReturn(Map.of("delivery.timeout.ms", 100));
        recoverer = new KafkaConfig().kafkaDeadLetterRecoverer(template);
        recoverer.setVerifyPartition(false); // 只禁止测试查询 Broker 元数据，保留真实路由与投递确认逻辑。
    }

    @Test
    @SuppressWarnings("unchecked")
    void preservesOriginalPartitionAndBusinessPayload() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        recoverer.accept(new ConsumerRecord<>("order-event-topic", 2, 42L, "O-1", event), null,
                new IllegalStateException("failed"));
        var captor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("order-event-topic.DLT");
        assertThat(captor.getValue().partition()).isEqualTo(2);
        assertThat(captor.getValue().value()).isSameAs(event);
    }

    @Test
    @SuppressWarnings("unchecked")
    void dltPublishFailureThrowsInsteadOfReportingRecovery() {
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("DLT unavailable")));
        assertThrows(RuntimeException.class, () -> recoverer.accept(
                new ConsumerRecord<>("order-event-topic", 1, 42L, "O-1", event), null, new IllegalStateException("business failed")));
    }
}
