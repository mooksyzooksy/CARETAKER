package com.tizitec.cdc.application.service;

import com.tizitec.cdc.domain.exception.DomainException;
import com.tizitec.cdc.domain.model.CustomEvent;
import com.tizitec.cdc.domain.model.InboxRecord;
import org.springframework.stereotype.Service;

@Service
public class ProcessingService {

    public CustomEvent process(InboxRecord record) {
        if (record.payload() == null || record.payload().isBlank()) {
            throw new DomainException("Empty payload for record id=" + record.id());
        }
        if (record.tableName() == null || record.operation() == null) {
            throw new DomainException("Missing tableName/operation for record id=" + record.id());
        }
        return new CustomEvent(
            record.scn(),
            record.tableName(),
            record.operation(),
            record.payload()
        );
    }
}
