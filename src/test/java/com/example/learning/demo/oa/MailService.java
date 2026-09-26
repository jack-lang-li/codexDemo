package com.example.learning.demo.oa;

/** Port for retrieving an unread-mail count in the OA dashboard example. */
public interface MailService {
    int getUnreadCount(String userId);
}
