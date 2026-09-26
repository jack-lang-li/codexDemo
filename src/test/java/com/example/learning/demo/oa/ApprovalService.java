package com.example.learning.demo.oa;

import java.util.List;

/** Port for retrieving pending approvals in the OA dashboard example. */
public interface ApprovalService {
    List<Approval> getPendingApprovals(String userId);
}
