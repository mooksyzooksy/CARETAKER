package com.tizitec.cdc.infrastructure.logminer;

/**
 * Mirrors the output shape of the external CDC library.
 *
 * <p>{@code rowSequence} is the intra-SCN ordinal: multiple DML operations
 * in a single Oracle transaction share the same SCN, and the library must
 * expose a strictly increasing ordinal within that SCN (typically derived
 * from {@code RS_ID} / {@code SSN} in {@code V$LOGMNR_CONTENTS}) so the
 * relay can preserve original transaction order end-to-end.
 *
 * <p>The stub client returns 0 because it never produces changes.
 */
public record CdcChange(
    String tableName,
    String operation,
    String payload,
    long scn,
    long rowSequence
) {}
