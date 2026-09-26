package com.example.learning.demo.oa;

import java.util.List;
import java.util.Objects;

/** Immutable response model for the OA dashboard aggregation example. */
public record OADashboardVO(
        List<Approval> approvals,
        List<Notice> notices,
        int unreadMailCount) {

    public OADashboardVO {
        approvals = List.copyOf(Objects.requireNonNull(approvals, "approvals"));
        notices = List.copyOf(Objects.requireNonNull(notices, "notices"));
        if (unreadMailCount < 0) {
            throw new IllegalArgumentException("unreadMailCount must not be negative");
        }
    }
}
