package com.tizitec.cdc.application.service;

import com.tizitec.cdc.domain.exception.DomainException;
import com.tizitec.cdc.domain.model.CustomEvent;
import com.tizitec.cdc.domain.model.InboxRecord;
import com.tizitec.cdc.domain.repository.InboxRepository;
import com.tizitec.cdc.infrastructure.kafka.KafkaEventPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class RelayService {

    private static final Logger log = LoggerFactory.getLogger(RelayService.class);

    private final InboxRepository inboxRepository;
    private final ProcessingService processingService;
    private final KafkaEventPublisher publisher;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate txTemplate;
    private final int batchSize;

    public RelayService(InboxRepository inboxRepository,
                        ProcessingService processingService,
                        KafkaEventPublisher publisher,
                        MeterRegistry meterRegistry,
                        PlatformTransactionManager txManager,
                        @Value("${cdc.relay.batch-size:500}") int batchSize) {
        this.inboxRepository = inboxRepository;
        this.processingService = processingService;
        this.publisher = publisher;
        this.meterRegistry = meterRegistry;
        this.txTemplate = new TransactionTemplate(txManager);
        this.batchSize = batchSize;
    }

    public void relay() {
        int processed;
        do {
            // Each batch runs in its own transaction so the SELECT FOR UPDATE SKIP LOCKED
            // lock is held for the full fetch → send → mark cycle. This is what prevents
            // a second instance from double-fetching the same NEW rows.
            processed = txTemplate.execute(status -> processBatch());
        } while (processed == batchSize);
    }

    private int processBatch() {
        List<InboxRecord> records = inboxRepository.findAllNew(batchSize);
        if (records.isEmpty()) return 0;

        List<Long> sent = new ArrayList<>();
        List<Long> failed = new ArrayList<>();
        List<Long> invalid = new ArrayList<>();

        // 1. Build event map for valid records (transformation failures → INVALID)
        Map<Long, CustomEvent> eventsById = new LinkedHashMap<>();
        for (InboxRecord record : records) {
            try {
                CustomEvent event = processingService.process(record);
                eventsById.put(record.id(), event);
            } catch (DomainException e) {
                log.warn("Invalid record id={} reason={}", record.id(), e.getMessage());
                invalid.add(record.id());
            }
        }

        // 2. Send all concurrently
        Map<Long, CompletableFuture<?>> futures = publisher.sendAll(eventsById);

        // 3. Wait once for the entire batch — swallow allOf exception, inspect individually below
        if (!futures.isEmpty()) {
            CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]))
                .exceptionally(ex -> null)
                .join();
        }

        // 4. Inspect each future independently for precise per-record outcome
        futures.forEach((id, future) -> {
            if (future.isCompletedExceptionally()) {
                log.error("Failed to relay record id={}", id);
                failed.add(id);
            } else {
                sent.add(id);
            }
        });

        if (!sent.isEmpty()) inboxRepository.markSent(sent);
        if (!failed.isEmpty()) inboxRepository.markFailed(failed);
        if (!invalid.isEmpty()) inboxRepository.markInvalid(invalid);

        log.info("Relay batch completed sent={} failed={} invalid={}",
            sent.size(), failed.size(), invalid.size());
        meterRegistry.counter("cdc.relay.sent").increment(sent.size());
        meterRegistry.counter("cdc.relay.failed").increment(failed.size());
        meterRegistry.counter("cdc.relay.invalid").increment(invalid.size());

        return records.size();
    }
}
