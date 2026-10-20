package com.example.learning.mq.common.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** 只作用于学习 Controller，参数错误返回 400，基础设施发送失败返回 503，不伪造发送成功。 */
@RestControllerAdvice(basePackages = "com.example.learning.mq")
public class MqDemoExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqDemoExceptionHandler.class);

    @ExceptionHandler({IllegalArgumentException.class, MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    ProblemDetail invalid(Exception exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "请求参数无效，请检查业务 ID、内容、数量或事件类型");
    }

    @ExceptionHandler(Exception.class)
    ProblemDetail failed(Exception exception) {
        LOGGER.error("[MQ-PRODUCER] result=FAILED，检查连接配置及 Broker 状态", exception);
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "MQ 发送失败或结果未确定，请核对日志和消息 ID；不要无条件重复提交业务");
    }
}
