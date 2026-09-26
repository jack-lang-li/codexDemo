package com.example.learning.demo.hr;

import java.time.LocalDate;
import java.util.Objects;

/** Minimal attendance data model passed through the import example. */
public record AttendanceRecord(String id, String employeeId, LocalDate attendanceDate) {
    public AttendanceRecord {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        if (employeeId == null || employeeId.isBlank()) {
            throw new IllegalArgumentException("employeeId must not be blank");
        }
        Objects.requireNonNull(attendanceDate, "attendanceDate");
    }
}
