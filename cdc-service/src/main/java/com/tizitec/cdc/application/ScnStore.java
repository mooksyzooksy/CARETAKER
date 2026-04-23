package com.tizitec.cdc.application;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class ScnStore {

    private final AtomicLong currentScn = new AtomicLong(-1);

    public long get() {
        return currentScn.get();
    }

    public void set(long scn) {
        currentScn.set(scn);
    }

    public boolean isInitialized() {
        return currentScn.get() != -1;
    }
}
