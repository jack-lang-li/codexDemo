package com.example.learning.demo.oa;

import java.util.List;

/** Port for retrieving recent announcements in the OA dashboard example. */
public interface NoticeService {
    List<Notice> getLatestNotices();
}
