package com.tizitec.cdc.domain.repository;

import com.tizitec.cdc.domain.model.Checkpoint;
import com.tizitec.cdc.domain.model.InboxRecord;

import java.util.List;

public interface InboxRepository {

    /**
     * Atomic: persists all records and the checkpoint in a single transaction.
     * Either both commit or both rollback.
     */
    void saveAll(List<InboxRecord> records, Checkpoint checkpoint);

    List<InboxRecord> findAllNew(int limit);

    void markSent(List<Long> ids);

    void markFailed(List<Long> ids);

    void markInvalid(List<Long> ids);

    void requeueFailed(int maxRetries);
}
