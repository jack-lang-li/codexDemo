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
    /**
     * 将原有内存商品目录交给 Spring 管理，供订单应用服务复用。
     * 保留 P-1001 的原价 19.90 和原有校验规则；这里只装配依赖，不发送消息、不连接数据库。
     */
    @Bean
    OrderService orderService() {
        // 当前项目尚无数据库商品目录，沿用原 LearningApplication 的内存商品。
        return new OrderService(List.of(new Product("P-1001", "Notebook", new BigDecimal("19.90"))));
    }
}
