package com.example.learning.domain.order;

import com.example.learning.domain.product.Product;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 原有订单领域服务：从内存商品目录校验商品和数量，创建保存价格快照的订单。
 * 领域层不依赖 MQ 或 Spring；需要发布事件时由 OrderApplicationService 在创建成功后负责。
 */
public final class OrderService {
    private final Map<String, Product> productsById;

    /**
     * 复制并冻结传入商品目录，保证调用方之后修改原集合不会影响订单计算。
     * 空目录元素或重复商品 ID 立即报错，避免下单时选中不确定的价格。
     */
    public OrderService(Collection<Product> products) {
        Objects.requireNonNull(products, "products");
        var catalog = new HashMap<String, Product>();
        for (var product : products) {
            Objects.requireNonNull(product, "product");
            if (catalog.putIfAbsent(product.getProductId(), product) != null) {
                throw new IllegalArgumentException("Duplicate productId: " + product.getProductId());
            }
        }
        // 防止调用方之后修改传入的 Collection 影响本服务的商品目录。
        this.productsById = Map.copyOf(catalog);
    }

    /**
     * 按商品 ID 查出当前商品，用其单价创建订单，由 Order 计算并保存总金额快照。
     * 此方法只执行原有内存领域规则，不持久化、不发送 MQ；未知商品和非正数量会抛异常。
     */
    public Order createOrder(String orderId, String buyerId, String productId, int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }

        var product = productsById.get(productId);
        if (product == null) {
            throw new IllegalArgumentException("Unknown productId: " + productId);
        }

        return new Order(orderId, buyerId, productId, quantity, product.getUnitPrice());
    }
}
