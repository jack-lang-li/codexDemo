package com.example.learning.domain.order;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * 原有不可变订单模型，保存下单时商品单价和总金额，后续商品调价不改变历史订单。
 * 应用层将其中订单号、买家和金额复制到 OrderCreatedEvent，避免直接把领域对象作为 MQ 协议。
 */
public final class Order {
    private final String orderId;
    private final String buyerId;
    private final String productId;
    private final int quantity;
    private final BigDecimal unitPriceAtOrder;
    private final BigDecimal totalAmount;

    /**
     * 创建订单并校验必填身份、正数数量和正数价格，总金额按 BigDecimal 单价乘数量计算。
     * 所有字段构造后不可变，便于业务与事件发布读取同一份订单快照。
     */
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

    /**
     * 返回订单业务编号，用作 MQ 事件的 businessId 和 Kafka 分区 key；与消息唯一 ID 不同。
     */
    public String getOrderId() {
        return orderId;
    }

    /**
     * 返回下单用户编号，发布订单事件时复制为快照中的 userId。
     */
    public String getBuyerId() {
        return buyerId;
    }

    /**
     * 返回本单商品编号，用于追踪购买的商品，不因商品信息后续变化而改变。
     */
    public String getProductId() {
        return productId;
    }

    /**
     * 返回已通过正数校验的购买数量，可用于订单展示或后续履约业务。
     */
    public int getQuantity() {
        return quantity;
    }

    /**
     * 返回下单时的单价快照；查看历史订单时不应重新查询商品现价来覆盖此值。
     */
    public BigDecimal getUnitPriceAtOrder() {
        return unitPriceAtOrder;
    }

    /**
     * 返回构造时计算的订单总金额，MQ 事件直接复制此结果，保持领域金额规则一致。
     */
    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    /**
     * 输出订单身份、购买数量和总额，供当前学习项目调试使用，不参与 JSON 消息协议。
     */
    @Override
    public String toString() {
        return "Order{orderId='" + orderId + "', buyerId='" + buyerId
                + "', productId='" + productId + "', quantity=" + quantity
                + ", totalAmount=" + totalAmount + '}';
    }

    /**
     * 统一拒绝 null 或空白身份字段，异常信息携带字段名，便于定位非法订单输入。
     */
    private static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
