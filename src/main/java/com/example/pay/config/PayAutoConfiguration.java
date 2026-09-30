package com.example.pay.config;

import com.example.pay.service.PayService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(PayProperties.class) // 启用属性绑定
@ConditionalOnProperty(prefix = "pay.sdk", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PayAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean // 如果用户自己定义了 PayService，则不重复创建
    public PayService payService(PayProperties properties) {
        return new PayService(properties.getAppId(), properties.getSecretKey());
    }
}
