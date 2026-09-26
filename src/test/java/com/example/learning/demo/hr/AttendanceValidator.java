package com.example.learning.demo.hr;

/** Port for the external attendance checks used by the import example. */
@FunctionalInterface
public interface AttendanceValidator {
    void validate(AttendanceRecord record) throws Exception;
}
