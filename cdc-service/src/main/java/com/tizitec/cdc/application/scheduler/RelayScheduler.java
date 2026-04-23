package com.tizitec.cdc.application.scheduler;

import com.tizitec.cdc.application.service.RelayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class RelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(RelayScheduler.class);

    private final RelayService relayService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RelayScheduler(RelayService relayService) {
        this.relayService = relayService;
    }

    @Scheduled(fixedDelayString = "${cdc.relay.interval-ms:3000}")
    public void run() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Relay already running, skipping tick");
            return;
        }
        try {
            relayService.relay();
        } finally {
            running.set(false);
        }
    }
}
