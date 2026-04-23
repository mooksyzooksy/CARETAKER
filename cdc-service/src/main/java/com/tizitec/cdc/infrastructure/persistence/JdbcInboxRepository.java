package com.tizitec.cdc.infrastructure.persistence;

import com.tizitec.cdc.domain.model.Checkpoint;
import com.tizitec.cdc.domain.model.InboxRecord;
import com.tizitec.cdc.domain.model.InboxStatus;
import com.tizitec.cdc.domain.repository.CheckpointRepository;
import com.tizitec.cdc.domain.repository.InboxRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public class JdbcInboxRepository implements InboxRepository {

    private static final String INSERT =
        "INSERT INTO CDC_INBOX (ID, TABLE_NAME, OPERATION, PAYLOAD, SCN, ROW_SEQUENCE, STATUS, RETRY_COUNT, CAPTURED_AT, UPDATED_AT) "
        + "VALUES (CDC_INBOX_SEQ.NEXTVAL, :tableName, :operation, :payload, :scn, :rowSequence, :status, :retryCount, :capturedAt, :updatedAt)";

    // ORDER BY SCN ASC, ROW_SEQUENCE ASC preserves original Oracle transaction order
    // even when multiple DML ops share the same SCN. ID is deliberately NOT part of
    // the sort (spec rule 15) — sequence cache gaps make it unreliable.
    private static final String SELECT_NEW =
        "SELECT ID, TABLE_NAME, OPERATION, PAYLOAD, SCN, ROW_SEQUENCE, STATUS, RETRY_COUNT, CAPTURED_AT, UPDATED_AT "
        + "FROM CDC_INBOX "
        + "WHERE STATUS = 'NEW' "
        + "ORDER BY SCN ASC, ROW_SEQUENCE ASC "
        + "FETCH FIRST :limit ROWS ONLY "
        + "FOR UPDATE SKIP LOCKED";

    private static final String MARK_SENT =
        "UPDATE CDC_INBOX SET STATUS = 'SENT', UPDATED_AT = SYSTIMESTAMP WHERE ID IN (:ids)";

    private static final String MARK_FAILED =
        "UPDATE CDC_INBOX SET STATUS = 'FAILED', RETRY_COUNT = RETRY_COUNT + 1, UPDATED_AT = SYSTIMESTAMP WHERE ID IN (:ids)";

    private static final String MARK_INVALID =
        "UPDATE CDC_INBOX SET STATUS = 'INVALID', UPDATED_AT = SYSTIMESTAMP WHERE ID IN (:ids)";

    private static final String REQUEUE_FAILED =
        "UPDATE CDC_INBOX SET STATUS = 'NEW', UPDATED_AT = SYSTIMESTAMP "
        + "WHERE STATUS = 'FAILED' AND RETRY_COUNT < :maxRetries";

    private final NamedParameterJdbcTemplate jdbc;
    private final CheckpointRepository checkpointRepository;

    public JdbcInboxRepository(NamedParameterJdbcTemplate jdbc,
                               CheckpointRepository checkpointRepository) {
        this.jdbc = jdbc;
        this.checkpointRepository = checkpointRepository;
    }

    @Override
    @Transactional
    public void saveAll(List<InboxRecord> records, Checkpoint checkpoint) {
        if (!records.isEmpty()) {
            SqlParameterSource[] batch = records.stream()
                .map(this::toParams)
                .toArray(SqlParameterSource[]::new);
            jdbc.batchUpdate(INSERT, batch);
        }
        checkpointRepository.save(checkpoint);
    }

    /**
     * MUST be called inside an ambient transaction. The {@code FOR UPDATE SKIP LOCKED}
     * lock is held only for the lifetime of the enclosing transaction — if this method
     * runs in its own short transaction, the locks are released immediately on return
     * and a second instance can double-fetch the same rows.
     *
     * RelayService owns the transaction boundary via TransactionTemplate so that the
     * lock spans fetch → process → mark* as a single unit.
     */
    @Override
    public List<InboxRecord> findAllNew(int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("limit", limit);
        return jdbc.query(SELECT_NEW, params, (rs, rowNum) -> new InboxRecord(
            rs.getLong("ID"),
            rs.getString("TABLE_NAME"),
            rs.getString("OPERATION"),
            rs.getString("PAYLOAD"),
            rs.getLong("SCN"),
            rs.getLong("ROW_SEQUENCE"),
            InboxStatus.valueOf(rs.getString("STATUS")),
            rs.getInt("RETRY_COUNT"),
            toLocalDateTime(rs.getTimestamp("CAPTURED_AT")),
            toLocalDateTime(rs.getTimestamp("UPDATED_AT"))
        ));
    }

    @Override
    @Transactional
    public void markSent(List<Long> ids) {
        if (ids.isEmpty()) return;
        jdbc.update(MARK_SENT, new MapSqlParameterSource("ids", ids));
    }

    @Override
    @Transactional
    public void markFailed(List<Long> ids) {
        if (ids.isEmpty()) return;
        jdbc.update(MARK_FAILED, new MapSqlParameterSource("ids", ids));
    }

    @Override
    @Transactional
    public void markInvalid(List<Long> ids) {
        if (ids.isEmpty()) return;
        jdbc.update(MARK_INVALID, new MapSqlParameterSource("ids", ids));
    }

    @Override
    @Transactional
    public void requeueFailed(int maxRetries) {
        jdbc.update(REQUEUE_FAILED, new MapSqlParameterSource("maxRetries", maxRetries));
    }

    private MapSqlParameterSource toParams(InboxRecord r) {
        return new MapSqlParameterSource()
            .addValue("tableName", r.tableName())
            .addValue("operation", r.operation())
            .addValue("payload", r.payload())
            .addValue("scn", r.scn())
            .addValue("rowSequence", r.rowSequence())
            .addValue("status", r.status().name())
            .addValue("retryCount", r.retryCount())
            .addValue("capturedAt", Timestamp.valueOf(r.capturedAt()))
            .addValue("updatedAt", Timestamp.valueOf(r.updatedAt()));
    }

    private static LocalDateTime toLocalDateTime(Timestamp ts) {
        return ts == null ? null : ts.toLocalDateTime();
    }
}
