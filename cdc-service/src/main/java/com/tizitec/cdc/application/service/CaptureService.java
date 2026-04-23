package com.tizitec.cdc.application.service;

import com.tizitec.cdc.application.ScnStore;
import com.tizitec.cdc.domain.model.Checkpoint;
import com.tizitec.cdc.domain.model.InboxRecord;
import com.tizitec.cdc.domain.model.ScnRange;
import com.tizitec.cdc.domain.repository.CheckpointRepository;
import com.tizitec.cdc.domain.repository.InboxRepository;
import com.tizitec.cdc.infrastructure.logminer.LogMinerClient;
import com.tizitec.cdc.infrastructure.logminer.LogMinerMapper;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CaptureService {

    private static final Logger log = LoggerFactory.getLogger(CaptureService.class);

    private final LogMinerClient logMinerClient;
    private final InboxRepository inboxRepository;
    private final CheckpointRepository checkpointRepository;
    private final ScnStore scnStore;
    private final MeterRegistry meterRegistry;

    public CaptureService(LogMinerClient logMinerClient,
                          InboxRepository inboxRepository,
                          CheckpointRepository checkpointRepository,
                          ScnStore scnStore,
                          MeterRegistry meterRegistry) {
        this.logMinerClient = logMinerClient;
        this.inboxRepository = inboxRepository;
        this.checkpointRepository = checkpointRepository;
        this.scnStore = scnStore;
        this.meterRegistry = meterRegistry;
    }

    public void capture() {
        // Exactly one currentScn() round-trip per tick. On first tick with an empty
        // checkpoint we reuse this value to seed the in-memory SCN; fromScn == toScn
        // then short-circuits fetchChanges to return nothing this tick.
        long toScn = logMinerClient.currentScn();

        if (!scnStore.isInitialized()) {
            long initialScn = checkpointRepository.findLatest()
                .map(Checkpoint::lastScn)
                .orElse(toScn);
            scnStore.set(initialScn);
            log.info("SCN initialized to {}", initialScn);
        }

        long fromScn = scnStore.get();
        log.info("Capture started fromScn={} toScn={}", fromScn, toScn);

        List<InboxRecord> records = logMinerClient
            .fetchChanges(new ScnRange(fromScn, toScn))
            .stream()
            .map(LogMinerMapper::toInboxRecord)
            .toList();

        if (!records.isEmpty()) {
            inboxRepository.saveAll(records, new Checkpoint(toScn));
        }

        scnStore.set(toScn);

        log.info("Capture completed recordsCaptured={} toScn={}", records.size(), toScn);
        meterRegistry.counter("cdc.capture.records").increment(records.size());
    }

    @PreDestroy
    public void onShutdown() {
        log.warn("CaptureService shutting down at scn={}", scnStore.get());
    }
}
