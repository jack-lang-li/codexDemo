package com.example.learning.domain.order;

import com.example.learning.domain.product.Product;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class OrderService {
    private final Map<String, Product> productsById;

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
