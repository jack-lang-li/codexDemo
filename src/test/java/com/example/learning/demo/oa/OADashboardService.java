package com.example.learning.demo.oa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** Aggregates the data needed by an OA dashboard using a bounded application executor. */
@Service
public class OADashboardService {
    private static final Logger LOGGER = LoggerFactory.getLogger(OADashboardService.class);

    private final ApprovalService approvalService;
    private final NoticeService noticeService;
    private final MailService mailService;
    private final Executor executor;

    public OADashboardService(
            ApprovalService approvalService,
            NoticeService noticeService,
            MailService mailService,
            @Qualifier("profileExecutor") Executor executor) {
        this.approvalService = approvalService;
        this.noticeService = noticeService;
        this.mailService = mailService;
        this.executor = executor;
    }

    public OADashboardVO getHomePageData(String userId) {
        CompletableFuture<List<Approval>> approvals = CompletableFuture.supplyAsync(
                () -> approvalService.getPendingApprovals(userId), executor);
        CompletableFuture<List<Notice>> notices = CompletableFuture.supplyAsync(
                noticeService::getLatestNotices, executor);
        CompletableFuture<Integer> unreadMailCount = CompletableFuture.supplyAsync(
                () -> mailService.getUnreadCount(userId), executor);

        try {
            CompletableFuture.allOf(approvals, notices, unreadMailCount).join();
            return new OADashboardVO(approvals.join(), notices.join(), unreadMailCount.join());
        } catch (CompletionException exception) {
            LOGGER.error("Failed to load OA dashboard data for user {}", userId, exception);
            throw new IllegalStateException("Failed to load OA dashboard data", exception.getCause());
        }
    }
}
