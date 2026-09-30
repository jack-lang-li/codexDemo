package com.example.pay.service;

public class PayService {
    private final String appId;
    private final String secretKey;

    public PayService(String appId, String secretKey) {
        this.appId = appId;
        this.secretKey = secretKey;
    }

    public void pay(double amount) {
        System.out.printf("[PaySDK] appId: %s 发起支付, 金额: %.2f 元%n", appId, amount);
    }
}
