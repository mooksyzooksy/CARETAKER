package com.tizitec.cdc.infrastructure.logminer;

import com.tizitec.cdc.domain.model.InboxRecord;
import com.tizitec.cdc.domain.model.InboxStatus;

import java.time.LocalDateTime;

public final class LogMinerMapper {

    private LogMinerMapper() {}

    public static InboxRecord toInboxRecord(CdcChange change) {
        LocalDateTime now = LocalDateTime.now();
        return new InboxRecord(
            null,
            change.tableName(),
            change.operation(),
            change.payload(),
            change.scn(),
            change.rowSequence(),
            InboxStatus.NEW,
            0,
            now,
            now
        );
    }
}
