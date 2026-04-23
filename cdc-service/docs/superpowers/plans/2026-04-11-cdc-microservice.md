# CDC Microservice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans for inline execution. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Java 21 / Spring Boot 3.x microservice that consumes change events from an external CDC library (stubbed) and publishes them to Kafka, with Oracle-backed inbox + checkpoint for durability.

**Architecture:** DDD-style layered (`domain/` / `application/` / `infrastructure/`). Two phases — **Capture** (LogMiner client → inbox + checkpoint, atomic) and **Relay** (inbox → process → Kafka concurrent batch). Failures separated into `FAILED` (retryable) vs `INVALID` (permanent). JDBC only via `NamedParameterJdbcTemplate`. `FOR UPDATE SKIP LOCKED` for multi-instance safety.

**Tech Stack:** Java 21, Spring Boot 3.5.x, Spring JDBC, Spring Kafka, Micrometer, Oracle JDBC (ojdbc11). No tests this phase.

**Maven coords:** `com.tizitec.cdc:cdc-service:0.0.1-SNAPSHOT`
**Target dir:** `C:\Users\tizit\projects\cdc-service`

---

## File Map

### Build + entrypoint
- `pom.xml`
- `src/main/resources/application.yml`
- `src/main/resources/db/schema.sql`
- `src/main/java/com/tizitec/cdc/CdcServiceApplication.java`

### domain/
- `domain/model/InboxStatus.java`
- `domain/model/ScnRange.java`
- `domain/model/Checkpoint.java`
- `domain/model/InboxRecord.java`
- `domain/model/CustomEvent.java`
- `domain/repository/CheckpointRepository.java`
- `domain/repository/InboxRepository.java`
- `domain/exception/DomainException.java`

### infrastructure/
- `infrastructure/logminer/CdcChange.java`
- `infrastructure/logminer/LogMinerClient.java`
- `infrastructure/logminer/DefaultLogMinerClient.java` (stub)
- `infrastructure/logminer/LogMinerMapper.java`
- `infrastructure/persistence/JdbcCheckpointRepository.java`
- `infrastructure/persistence/JdbcInboxRepository.java`
- `infrastructure/kafka/KafkaEventPublisher.java`
- `infrastructure/config/CdcConfig.java`
- `infrastructure/config/SchedulingConfig.java`

### application/
- `application/ScnStore.java`
- `application/service/ProcessingService.java`
- `application/service/CaptureService.java`
- `application/service/RelayService.java`
- `application/scheduler/CaptureScheduler.java`
- `application/scheduler/RelayScheduler.java`
- `application/scheduler/RequeueScheduler.java`

---

## Task 1 — Project scaffold

- [ ] Create `pom.xml` (Spring Boot 3.5.x parent, Java 21, starter/jdbc/kafka, micrometer-prometheus, ojdbc11)
- [ ] Create `CdcServiceApplication.java` with `@SpringBootApplication`
- [ ] Create `application.yml` with cdc.* + spring.datasource + spring.kafka blocks per spec
- [ ] Create `schema.sql` with `CDC_INBOX_SEQ`, `CDC_INBOX`, `CDC_CHECKPOINT` DDL
- [ ] Run `mvn -q -DskipTests compile` — expect SUCCESS (empty project compiles)

## Task 2 — Domain layer

- [ ] `InboxStatus.java` — enum NEW/SENT/FAILED/INVALID
- [ ] `ScnRange.java` — record(long fromScn, long toScn)
- [ ] `Checkpoint.java` — record(long lastScn)
- [ ] `InboxRecord.java` — record with id, tableName, operation, payload, scn, status, retryCount, capturedAt, updatedAt
- [ ] `CustomEvent.java` — record(long scn, String tableName, String operation, Object data)
- [ ] `DomainException.java` — RuntimeException with (msg) + (msg, cause) ctors
- [ ] `CheckpointRepository.java` — interface with findLatest() + save()
- [ ] `InboxRepository.java` — interface with saveAll/findAllNew/markSent/markFailed/markInvalid/requeueFailed

## Task 3 — Infrastructure: LogMiner stub

- [ ] `CdcChange.java` (infra package) — record per spec
- [ ] `LogMinerClient.java` — interface with `currentScn()` + `fetchChanges(ScnRange)`
- [ ] `DefaultLogMinerClient.java` — `@Component` stub: returns 0L for SCN, empty list for fetch. Log warn on first call.
- [ ] `LogMinerMapper.java` — static `toInboxRecord(CdcChange)` mapper

## Task 4 — Infrastructure: Persistence

- [ ] `JdbcCheckpointRepository.java` — `@Repository` with `NamedParameterJdbcTemplate`. `findLatest()` returns empty on empty table. `save()` uses Oracle `MERGE INTO CDC_CHECKPOINT USING DUAL ON (1=1)`.
- [ ] `JdbcInboxRepository.java` — `@Repository` with all methods per spec. `saveAll` `@Transactional` calls checkpointRepo.save in same tx. `findAllNew` uses `FOR UPDATE SKIP LOCKED`. Multi-id updates via `:ids` named param.

## Task 5 — Infrastructure: Kafka + Config

- [ ] `KafkaEventPublisher.java` — `@Component` with `sendAll(Map<Long,CustomEvent>)` returning `Map<Long,CompletableFuture<?>>`. Topic from `@Value("${cdc.kafka.topic}")`.
- [ ] `SchedulingConfig.java` — `@Configuration` + `@EnableScheduling` + `SchedulingConfigurer`. Dedicated `cdcTaskScheduler` bean (poolSize=3, prefix `cdc-scheduler-`, wait-for-tasks, 30s termination, error handler).
- [ ] `CdcConfig.java` — `@Configuration`, holds `@Value` batchSize + maxRetries accessors via `@Bean`s (`CdcProperties`). No `@EnableScheduling`.

## Task 6 — Application: ScnStore + ProcessingService

- [ ] `ScnStore.java` — `@Component`, `AtomicLong` initialized to -1, `get`/`set`/`isInitialized`
- [ ] `ProcessingService.java` — `@Service`. Stub process: build `CustomEvent(scn, tableName, operation, payload)`. Throw `DomainException` on null payload.

## Task 7 — Application: CaptureService

- [ ] `CaptureService.java` — `@Service`. Implement `capture()` per spec (bootstrap SCN, fetch, map, saveAll, advance SCN). `@PreDestroy` shutdown log. Micrometer `cdc.capture.records` counter.

## Task 8 — Application: RelayService

- [ ] `RelayService.java` — `@Service`. Drain loop with `batchSize` from `@Value`. Per-future batch pattern (NOT sync loop): build `eventsById`, call `publisher.sendAll`, wait via `CompletableFuture.allOf(...).exceptionally(null)`, inspect each future individually. Update SENT/FAILED/INVALID lists. Counters cdc.relay.sent/failed/invalid.

## Task 9 — Application: Schedulers

- [ ] `CaptureScheduler.java` — `@Component`, `AtomicBoolean` guard, `@Scheduled(fixedDelayString="${cdc.capture.interval-ms:5000}")`
- [ ] `RelayScheduler.java` — same pattern with `${cdc.relay.interval-ms:3000}`
- [ ] `RequeueScheduler.java` — same pattern with `${cdc.requeue.interval-ms:60000}`, reads `${cdc.requeue.max-retries:5}`, calls `inboxRepository.requeueFailed(maxRetries)`

## Task 10 — Verify compile

- [ ] Run `mvn -q -DskipTests compile` in project dir — expect SUCCESS
- [ ] Run `mvn -q -DskipTests package` — expect SUCCESS, boot jar built
