package com.tizitec.cdc.application.scheduler;

import com.tizitec.cdc.application.service.CaptureService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class CaptureScheduler {

    private static final Logger log = LoggerFactory.getLogger(CaptureScheduler.class);

    private final CaptureService captureService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public CaptureScheduler(CaptureService captureService) {
        this.captureService = captureService;
    }

    @Scheduled(fixedDelayString = "${cdc.capture.interval-ms:5000}")
    public void run() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Capture already running, skipping tick");
            return;
        }
        try {
            captureService.capture();
        } finally {
            running.set(false);
        }
    }
}
