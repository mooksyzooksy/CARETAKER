package com.tizitec.cdc.infrastructure.logminer;

import com.tizitec.cdc.domain.model.ScnRange;

import java.util.List;

public interface LogMinerClient {

    long currentScn();

    List<CdcChange> fetchChanges(ScnRange range);
}
