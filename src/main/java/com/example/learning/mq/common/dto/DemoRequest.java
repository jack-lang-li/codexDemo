package com.example.learning.mq.common.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** HTTP 请求与传输事件分开：messageId、时间和金额均由服务端生成，不能由客户端伪造。 */
@Schema(description = "MQ 学习请求；订单示例固定使用商品 P-1001，单价 19.90")
public record DemoRequest(
        @NotBlank @Size(max = 100) @Schema(description = "订单、用户或任务的业务 ID", example = "O-1001") String businessId,
        @NotBlank @Size(max = 1000) @Schema(description = "学习消息内容", example = "订单已创建") String content,
        @Schema(description = "Topic 路由：order.created / order.paid / order.cancelled / order.payment.completed") String eventType,
        @Size(max = 100) @Schema(description = "订单购买者；省略时为 U-DEMO") String userId,
        @Min(1) @Max(100) @Schema(description = "购买数量；省略时为 1") Integer quantity) {
}
