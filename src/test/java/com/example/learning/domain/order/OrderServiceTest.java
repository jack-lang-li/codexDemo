package com.example.learning.domain.order;

import com.example.learning.domain.product.Product;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 保留原有订单领域规则的回归测试：验证价格快照、总金额、数量及商品校验。
 * 不依赖 MQ；与真实服务器案例一起运行，确保本次接入未改变原有订单行为。
 */
class OrderServiceTest {
    private final OrderService orderService = new OrderService(List.of(
            new Product("P-1001", "Notebook", new BigDecimal("19.90"))));

    /**
     * 核对两件 19.90 商品的总额为 39.80，并保存下单单价，防止 MQ 接入改动原有金额规则。
     */
    @Test
    void calculatesOrderTotalFromThePriceAtOrderTime() {
        var order = orderService.createOrder("O-1001", "U-1001", "P-1001", 2);

        assertEquals(new BigDecimal("39.80"), order.getTotalAmount());
        assertEquals(new BigDecimal("19.90"), order.getUnitPriceAtOrder());
    }

    /**
     * 分别以零和负数下单，验证原领域服务仍拒绝无效数量，不允许产生非法订单或后续事件。
     */
    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectsZeroOrNegativeQuantity(int quantity) {
        assertThrows(IllegalArgumentException.class,
                () -> orderService.createOrder("O-1002", "U-1001", "P-1001", quantity));
    }

    /**
     * 使用目录中不存在的商品下单，验证在创建订单阶段报错，不误用默认价格继续业务。
     */
    @Test
    void rejectsProductsThatAreNotInTheInMemoryCatalog() {
        assertThrows(IllegalArgumentException.class,
                () -> orderService.createOrder("O-1003", "U-1001", "missing", 1));
    }
}
