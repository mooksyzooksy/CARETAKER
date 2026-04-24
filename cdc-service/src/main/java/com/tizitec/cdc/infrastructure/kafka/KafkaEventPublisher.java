package com.tizitec.cdc.infrastructure.kafka;

import com.tizitec.cdc.domain.model.CustomEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes CDC events to Kafka with an at-least-once delivery guarantee and a
 * stable idempotency key so downstream consumers can dedupe across producer restarts.
 *
 * <h2>Delivery guarantee</h2>
 * The transactional outbox pattern used here (Oracle inbox → Kafka relay → mark SENT)
 * cannot provide exactly-once delivery on its own: the Kafka broker ack and the
 * DB {@code markSent} are not atomic. A crash after broker ack but before DB commit
 * leads to re-publication of the same record on the next tick. This is intrinsic to
 * outbox + Kafka and cannot be eliminated on the producer side without giving up
 * per-record accounting — which rules out Kafka producer transactions at the batch level.
 *
 * <h2>Consumer contract — REQUIRED</h2>
 * Consumers MUST dedupe on the {@link #HEADER_IDEMPOTENCY_KEY} header. The value is
 * {@code {tableName}:{scn}:{rowSequence}} and is globally unique per Oracle change event.
 * A consumer that has already processed a given key MUST skip the duplicate silently.
 *
 * <h2>Why (tableName, scn, rowSequence)</h2>
 * Oracle SCN is monotonic per database, but a single SCN can cover multiple DML
 * operations on the same table (e.g. two UPDATEs in the same commit). Using only
 * {@code (tableName, scn)} as the key would produce a collision for the second operation,
 * causing consumers to silently discard it. {@code rowSequence} is the intra-SCN
 * position assigned by the LogMiner library and disambiguates all operations within
 * the same SCN. The triple {@code (tableName, scn, rowSequence)} is guaranteed unique
 * and survives producer restarts — a re-publication always carries the same key.
 *
 * <h2>Ordering caveat</h2>
 * The Kafka message key is {@code tableName}, so all changes for one table land on the
 * same partition and Kafka preserves their publication order. Retries of failed records
 * can re-interleave with later records that already succeeded. Consumers requiring
 * strict per-row order must dedupe on the idempotency key AND track the highest SCN
 * (and rowSequence) applied per {@code (tableName, primaryKey)}.
 *
 * <h2>Headers emitted</h2>
 * <ul>
 *   <li>{@value #HEADER_IDEMPOTENCY_KEY} — {@code {tableName}:{scn}:{rowSequence}}</li>
 *   <li>{@value #HEADER_SCN}             — Oracle SCN as a decimal string</li>
 *   <li>{@value #HEADER_ROW_SEQUENCE}    — intra-SCN position as a decimal string</li>
 *   <li>{@value #HEADER_TABLE}           — source table name</li>
 *   <li>{@value #HEADER_OPERATION}       — INSERT / UPDATE / DELETE</li>
 * </ul>
 */
@Component
public class KafkaEventPublisher {

    public static final String HEADER_IDEMPOTENCY_KEY = "cdc-idempotency-key";
    public static final String HEADER_SCN             = "cdc-scn";
    public static final String HEADER_ROW_SEQUENCE    = "cdc-row-sequence";
    public static final String HEADER_TABLE           = "cdc-table";
    public static final String HEADER_OPERATION       = "cdc-operation";

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public KafkaEventPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                               @Value("${cdc.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Sends all events concurrently. Returns a map of record id → future so the caller
     * can await the whole batch once and inspect each outcome individually.
     *
     * <p>Synchronous failures (serialization errors, interceptor failures, closed producer)
     * are trapped per record and exposed as a pre-failed {@link CompletableFuture} so the
     * relay's per-record accounting stays correct — the batch never aborts mid-loop.
     */
    public Map<Long, CompletableFuture<?>> sendAll(Map<Long, CustomEvent> eventsById) {
        Map<Long, CompletableFuture<?>> futures = new LinkedHashMap<>();
        eventsById.forEach((id, event) -> {
            try {
                futures.put(id, kafkaTemplate.send(buildRecord(event)));
            } catch (Exception e) {
                log.error("Synchronous send failure for record id={}", id, e);
                futures.put(id, CompletableFuture.failedFuture(e));
            }
        });
        return futures;
    }

    private ProducerRecord<String, Object> buildRecord(CustomEvent event) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            topic,
            null,               // partition — let the partitioner decide from the key
            event.tableName(),  // key — per-table partition affinity preserves SCN order
            event               // value — serialized as JSON by the configured serializer
        );

        // (tableName, scn, rowSequence) is the collision-free idempotency triple.
        // rowSequence disambiguates multiple DML ops sharing the same SCN on the same table.
        String idempotencyKey = event.tableName() + ":" + event.scn() + ":" + event.rowSequence();

        record.headers()
            .add(HEADER_IDEMPOTENCY_KEY, idempotencyKey.getBytes(StandardCharsets.UTF_8))
            .add(HEADER_SCN,          Long.toString(event.scn()).getBytes(StandardCharsets.UTF_8))
            .add(HEADER_ROW_SEQUENCE, Long.toString(event.rowSequence()).getBytes(StandardCharsets.UTF_8))
            .add(HEADER_TABLE,        event.tableName().getBytes(StandardCharsets.UTF_8))
            .add(HEADER_OPERATION,    event.operation().getBytes(StandardCharsets.UTF_8));

        return record;
    }
}
