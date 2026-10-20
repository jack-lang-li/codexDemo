package com.example.learning.mq;

import com.example.learning.mq.common.service.RedisMessageIdempotencyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 不连接 Redis：验证最容易导致消息丢失的成功/失败/处理中/租约失效状态转换。 */
class RedisMessageIdempotencyServiceTest {
    private StringRedisTemplate redis;
    private ValueOperations<String, String> values;
    private RedisMessageIdempotencyService service;
    private final String key = "learning:mq:consumed:orders:m-1";

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        service = new RedisMessageIdempotencyService(redis, 300, 86400);
    }

    @Test
    void runsOnceAndSkipsOnlyCompletedDuplicate() {
        Runnable business = mock(Runnable.class);
        when(values.setIfAbsent(eq(key), anyString(), eq(Duration.ofSeconds(300)))).thenReturn(true, false);
        when(values.get(key)).thenReturn("DONE");
        when(redis.execute(org.mockito.ArgumentMatchers.<RedisScript<Long>>any(), eq(List.of(key)), any(), eq("86400000")))
                .thenReturn(1L);

        assertTrue(service.execute("orders", "m-1", business));
        assertFalse(service.execute("orders", "m-1", business));
        verify(business, times(1)).run();
    }

    @Test
    void processingIsRetriedRatherThanAcknowledgedAsSuccess() {
        when(values.setIfAbsent(eq(key), anyString(), any(Duration.class))).thenReturn(false);
        when(values.get(key)).thenReturn("another-processing-token");
        Runnable business = mock(Runnable.class);
        assertThrows(IllegalStateException.class, () -> service.execute("orders", "m-1", business));
        verifyNoInteractions(business);
    }

    @Test
    void failedBusinessReleasesOwnedTokenWithoutMarkingDone() {
        when(values.setIfAbsent(eq(key), anyString(), any(Duration.class))).thenReturn(true);
        RuntimeException failure = new IllegalStateException("database failed");
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> service.execute("orders", "m-1", () -> { throw failure; })));
        verify(redis).execute(org.mockito.ArgumentMatchers.<RedisScript<Long>>any(), eq(List.of(key)), anyString());
        verify(redis, never()).execute(org.mockito.ArgumentMatchers.<RedisScript<Long>>any(), eq(List.of(key)), any(), eq("86400000"));
    }

    @Test
    void lostLeaseCannotBeReportedAsSuccessfulConsumption() {
        when(values.setIfAbsent(eq(key), anyString(), any(Duration.class))).thenReturn(true);
        when(redis.execute(org.mockito.ArgumentMatchers.<RedisScript<Long>>any(), eq(List.of(key)), any(), eq("86400000")))
                .thenReturn(0L);
        assertThrows(IllegalStateException.class, () -> service.execute("orders", "m-1", () -> { }));
    }
}
