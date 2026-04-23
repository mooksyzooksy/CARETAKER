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
 * per-record accounting (rule 17 — never fail the whole batch on a single record
 * failure), which rules out Kafka producer transactions at the batch level.
 *
 * <h2>Consumer contract — REQUIRED</h2>
 * Consumers MUST dedupe on the {@link #HEADER_IDEMPOTENCY_KEY} header. The value is
 * {@code {tableName}:{scn}} and is globally unique per Oracle change. A consumer that
 * has already processed a given idempotency key MUST skip the duplicate.
 *
 * <h2>Why (tableName, scn)</h2>
 * Oracle SCN is monotonic per database; tableName + scn uniquely identifies a logical
 * change. Both survive producer restarts, so a re-publication always carries the same
 * key as the original — consumers can dedupe without any producer-side state.
 *
 * <h2>Ordering caveat</h2>
 * The Kafka message key is {@code tableName}, so all changes for one table land on the
 * same partition and Kafka preserves their order. Retries of failed records, however,
 * can re-interleave: a record at {@code scn=N} may be re-sent after a later record at
 * {@code scn=N+1} has already succeeded. Consumers that require strict per-row order
 * must dedupe on idempotency key AND track the highest SCN they have applied per
 * {@code (tableName, primaryKey)}.
 *
 * <h2>Headers emitted</h2>
 * <ul>
 *   <li>{@value #HEADER_IDEMPOTENCY_KEY} — {@code {tableName}:{scn}}, the dedupe key</li>
 *   <li>{@value #HEADER_SCN} — the Oracle SCN as a decimal string</li>
 *   <li>{@value #HEADER_TABLE} — source table name</li>
 *   <li>{@value #HEADER_OPERATION} — INSERT / UPDATE / DELETE</li>
 * </ul>
 */
@Component
public class KafkaEventPublisher {

    public static final String HEADER_IDEMPOTENCY_KEY = "cdc-idempotency-key";
    public static final String HEADER_SCN = "cdc-scn";
    public static final String HEADER_TABLE = "cdc-table";
    public static final String HEADER_OPERATION = "cdc-operation";

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
     * Synchronous failures (serialization errors, interceptor failures, closed producer)
     * are trapped per record and exposed as a pre-failed CompletableFuture so the relay's
     * per-record accounting stays correct even for sync faults — the batch never aborts
     * mid-loop.
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
            null,                    // partition — let the partitioner decide from the key
            event.tableName(),       // key — per-table partition affinity preserves order
            event                    // value — serialized as JSON by the configured serializer
        );
        String idempotencyKey = event.tableName() + ":" + event.scn();
        record.headers()
            .add(HEADER_IDEMPOTENCY_KEY, idempotencyKey.getBytes(StandardCharsets.UTF_8))
            .add(HEADER_SCN, Long.toString(event.scn()).getBytes(StandardCharsets.UTF_8))
            .add(HEADER_TABLE, event.tableName().getBytes(StandardCharsets.UTF_8))
            .add(HEADER_OPERATION, event.operation().getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
