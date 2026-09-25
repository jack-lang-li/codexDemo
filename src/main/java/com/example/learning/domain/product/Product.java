package com.example.learning.domain.product;

import java.math.BigDecimal;
import java.util.Objects;

public final class Product {
    private final String productId;
    private final String name;
    private final BigDecimal unitPrice;

    public Product(String productId, String name, BigDecimal unitPrice) {
        this.productId = requireText(productId, "productId");
        this.name = requireText(name, "name");
        this.unitPrice = Objects.requireNonNull(unitPrice, "unitPrice");
        if (unitPrice.signum() <= 0) {
            throw new IllegalArgumentException("unitPrice must be positive");
        }
    }

    public String getProductId() {
        return productId;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

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

    @Override
    public int hashCode() {
        return productId.hashCode();
    }

    @Override
    public String toString() {
        return "Product{productId='" + productId + "', name='" + name
                + "', unitPrice=" + unitPrice + '}';
    }

    private static String requireText(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }
}
