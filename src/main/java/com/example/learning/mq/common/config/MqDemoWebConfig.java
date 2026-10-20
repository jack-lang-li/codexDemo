package com.example.learning.mq.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/**
 * 仅供独立学习入口使用，mq profile 默认绑定 127.0.0.1。
 * 开放演示 API/文档以便 Postman 调试；只对演示 POST 忽略 CSRF。
 * 暴露到共享环境之前，应替换为项目认证授权，尤其限制故障注入接口。
 */
@Configuration(proxyBeanMethods = false)
public class MqDemoWebConfig {
    @Bean
    SecurityFilterChain mqDemoSecurity(HttpSecurity http) throws Exception {
        return http.authorizeHttpRequests(auth -> auth
                        .requestMatchers("/demo/**", "/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html", "/error").permitAll()
                        .anyRequest().authenticated())
                .csrf(csrf -> csrf.ignoringRequestMatchers("/demo/**"))
                .build();
    }
}
