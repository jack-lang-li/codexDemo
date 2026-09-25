package com.example.learning.domain.order;

import java.math.BigDecimal;
import java.util.Objects;

public final class Order {
    private final String orderId;
    private final String buyerId;
    private final String productId;
    private final int quantity;
    private final BigDecimal unitPriceAtOrder;
    private final BigDecimal totalAmount;

    public Order(String orderId, String buyerId, String productId,
                 int quantity, BigDecimal unitPriceAtOrder) {
        this.orderId = requireText(orderId, "orderId");
        this.buyerId = requireText(buyerId, "buyerId");
        this.productId = requireText(productId, "productId");
        this.unitPriceAtOrder = Objects.requireNonNull(unitPriceAtOrder, "unitPriceAtOrder");
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
        if (unitPriceAtOrder.signum() <= 0) {
            throw new IllegalArgumentException("unitPriceAtOrder must be positive");
        }

        this.quantity = quantity;
        // 订单保存下单时的单价快照，商品之后调价不会改写历史订单金额。
        this.totalAmount = unitPriceAtOrder.multiply(BigDecimal.valueOf(quantity));
    }

    public String getOrderId() {
        return orderId;
    }

    public String getBuyerId() {
        return buyerId;
    }

    public String getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getUnitPriceAtOrder() {
        return unitPriceAtOrder;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    @Override
    public String toString() {
        return "Order{orderId='" + orderId + "', buyerId='" + buyerId
                + "', productId='" + productId + "', quantity=" + quantity
                + ", totalAmount=" + totalAmount + '}';
    }

    private static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
