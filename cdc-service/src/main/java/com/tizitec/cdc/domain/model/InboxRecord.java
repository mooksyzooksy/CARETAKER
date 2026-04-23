package com.tizitec.cdc.domain.model;

import java.time.LocalDateTime;

public record InboxRecord(
    Long id,
    String tableName,
    String operation,
    String payload,
    long scn,
    long rowSequence,
    InboxStatus status,
    int retryCount,
    LocalDateTime capturedAt,
    LocalDateTime updatedAt
) {}
