package com.example.learning.domain.product;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * 原有不可变商品模型，保存商品编号、名称和当前单价，供 OrderService 建立内存目录。
 * 商品以编号判断身份；下单时订单复制单价，消息随后携带订单金额快照。
 */
public final class Product {
    private final String productId;
    private final String name;
    private final BigDecimal unitPrice;

    /**
     * 校验商品身份、名称与正数单价，拒绝不完整目录项进入订单领域服务。
     * 使用 BigDecimal 表达金额，避免二进制浮点数计算价格带来的精度误差。
     */
    public Product(String productId, String name, BigDecimal unitPrice) {
        this.productId = requireText(productId, "productId");
        this.name = requireText(name, "name");
        this.unitPrice = Objects.requireNonNull(unitPrice, "unitPrice");
        if (unitPrice.signum() <= 0) {
            throw new IllegalArgumentException("unitPrice must be positive");
        }
    }

    /**
     * 返回稳定商品编号，作为商品目录的查询键及商品对象相等性的依据。
     */
    public String getProductId() {
        return productId;
    }

    /**
     * 返回已校验非空白的商品名称，供业务展示，不参与商品身份判断。
     */
    public String getName() {
        return name;
    }

    /**
     * 返回商品单价，下单时由 OrderService 复制到订单作为价格快照。
     */
    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    /**
     * 按商品编号判断是否为同一商品，与名称、当前价格无关，避免调价改变商品身份。
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Product product)) {
            return false;
        }
        return productId.equals(product.productId);
    }

    /**
     * 根据商品编号生成散列值，与 equals 使用相同身份规则，保证集合中的查找行为一致。
     */
    @Override
    public int hashCode() {
        return productId.hashCode();
    }

    /**
     * 输出商品编号、名称和单价，供内存目录调试使用，不承担消息序列化职责。
     */
    @Override
    public String toString() {
        return "Product{productId='" + productId + "', name='" + name
                + "', unitPrice=" + unitPrice + '}';
    }

    /**
     * 统一拒绝 null 或空白文本，异常中保留字段名，便于定位非法商品数据。
     */
    private static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
