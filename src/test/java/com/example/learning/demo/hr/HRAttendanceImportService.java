package com.example.learning.demo.hr;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Runs attendance validation work on a bounded, named thread pool. */
@Service
public class HRAttendanceImportService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HRAttendanceImportService.class);
    private static final AtomicInteger THREAD_NUMBER = new AtomicInteger(1);

    private final AttendanceValidator attendanceValidator;
    private final ThreadPoolExecutor importPool = new ThreadPoolExecutor(
            4,
            10,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1_000),
            attendanceThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy());

    public HRAttendanceImportService(AttendanceValidator attendanceValidator) {
        this.attendanceValidator = Objects.requireNonNull(attendanceValidator, "attendanceValidator");
    }

    public void processExcelData(List<AttendanceRecord> records) {
        Objects.requireNonNull(records, "records");
        for (AttendanceRecord record : records) {
            Objects.requireNonNull(record, "record");
            importPool.execute(() -> {
                try {
                    attendanceValidator.validate(record);
                } catch (Exception exception) {
                    LOGGER.error("Attendance record validation failed: {}", record.id(), exception);
                }
            });
        }
    }

    @PreDestroy
    public void shutdown() {
        importPool.shutdown();
        try {
            if (!importPool.awaitTermination(30, TimeUnit.SECONDS)) {
                importPool.shutdownNow();
            }
        } catch (InterruptedException exception) {
            importPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory attendanceThreadFactory() {
        return task -> new Thread(task, "HR-Attendance-Pool-" + THREAD_NUMBER.getAndIncrement());
    }
}
