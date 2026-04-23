package com.tizitec.cdc.domain.model;

public record CustomEvent(
    long scn,
    String tableName,
    String operation,
    Object data
) {}
