package com.tizitec.cdc.application.scheduler;

import com.tizitec.cdc.domain.repository.InboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class RequeueScheduler {

    private static final Logger log = LoggerFactory.getLogger(RequeueScheduler.class);

    private final InboxRepository inboxRepository;
    private final int maxRetries;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RequeueScheduler(InboxRepository inboxRepository,
                            @Value("${cdc.requeue.max-retries:5}") int maxRetries) {
        this.inboxRepository = inboxRepository;
        this.maxRetries = maxRetries;
    }

    @Scheduled(fixedDelayString = "${cdc.requeue.interval-ms:60000}")
    public void run() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Requeue already running, skipping tick");
            return;
        }
        try {
            inboxRepository.requeueFailed(maxRetries);
            log.info("Requeue completed for records under maxRetries={}", maxRetries);
        } finally {
            running.set(false);
        }
    }
}
