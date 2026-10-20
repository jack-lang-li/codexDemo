package com.example.learning.config;

import com.example.learning.domain.order.OrderService;
import com.example.learning.domain.product.Product;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.List;

/** 将原 main 中的内存商品目录和 OrderService 注册到统一服务，保持原金额与校验规则。 */
@Configuration(proxyBeanMethods = false)
public class OrderConfig {
    @Bean
    OrderService orderService() {
        // 当前项目尚无数据库商品目录，沿用原 LearningApplication 的内存商品。
        return new OrderService(List.of(new Product("P-1001", "Notebook", new BigDecimal("19.90"))));
    }
}
