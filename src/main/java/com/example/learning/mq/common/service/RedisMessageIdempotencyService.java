package com.example.learning.mq.common.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Redis SETNX 学习版幂等：先占用 PROCESSING token，业务成功后再原子改为 DONE。
 * 失败使用比较 token 的 Lua 删除锁，防止误删其他消费者的新锁。
 * scope 必须包含订阅者身份：广播的邮件/短信，以及 Kafka 的不同 group，均需独立处理。
 *
 * 边界：Redis 与数据库并非同一事务。业务成功后进程崩溃、租约过期、Redis 数据丢失，
 * 仍可能重复执行业务。真实扣款应以数据库唯一索引/消费记录+业务事务兜底；长任务需续租。
 */
@Service
public class RedisMessageIdempotencyService {
    private static final DefaultRedisScript<Long> COMPLETE = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "redis.call('psetex', KEYS[1], ARGV[2], 'DONE'); return 1 else return 0 end", Long.class);
    private static final DefaultRedisScript<Long> RELEASE = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end", Long.class);
    private final StringRedisTemplate redis;
    private final Duration lease;
    private final Duration retention;

    public RedisMessageIdempotencyService(StringRedisTemplate redis,
            @Value("${app.mq.idempotency.lease-seconds:300}") long leaseSeconds,
            @Value("${app.mq.idempotency.retention-seconds:86400}") long retentionSeconds) {
        if (leaseSeconds <= 0 || retentionSeconds <= 0) {
            throw new IllegalArgumentException("幂等过期时间必须大于 0");
        }
        this.redis = redis;
        this.lease = Duration.ofSeconds(leaseSeconds);
        this.retention = Duration.ofSeconds(retentionSeconds);
    }

    /** @return true 表示本次执行业务，false 表示已完成的重复消息。Redis 异常直接抛出，绝不误 ACK。 */
    public boolean execute(String scope, String messageId, Runnable business) {
        String key = "learning:mq:consumed:" + scope + ":" + messageId;
        String token = UUID.randomUUID().toString();
        if (!Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(key, token, lease))) {
            if ("DONE".equals(redis.opsForValue().get(key))) {
                return false;
            }
            // 正在处理不能等同于成功；让框架有限重试，避免原消费者失败时丢失这次投递。
            throw new IllegalStateException("消息正在处理，请稍后重试: " + messageId);
        }
        try {
            business.run();
            Long completed = redis.execute(COMPLETE, List.of(key), token, String.valueOf(retention.toMillis()));
            if (!Long.valueOf(1).equals(completed)) {
                throw new IllegalStateException("处理租约已过期，需核对业务状态: " + messageId);
            }
            return true;
        } catch (RuntimeException exception) {
            try {
                redis.execute(RELEASE, List.of(key), token);
            } catch (RuntimeException cleanupFailure) {
                exception.addSuppressed(cleanupFailure);
            }
            throw exception;
        }
    }
}
