package com.tizitec.cdc.infrastructure.logminer;

import com.tizitec.cdc.domain.model.ScnRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class DefaultLogMinerClient implements LogMinerClient {

    private static final Logger log = LoggerFactory.getLogger(DefaultLogMinerClient.class);

    private final AtomicBoolean warned = new AtomicBoolean(false);

    @Override
    public long currentScn() {
        warnOnce();
        return 0L;
    }

    @Override
    public List<CdcChange> fetchChanges(ScnRange range) {
        warnOnce();
        return Collections.emptyList();
    }

    private void warnOnce() {
        if (warned.compareAndSet(false, true)) {
            log.warn("DefaultLogMinerClient is a STUB — no real CDC library wired in. "
                + "Replace with adapter that delegates to the external LogMiner library.");
        }
    }
}
