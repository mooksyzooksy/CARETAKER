package com.tizitec.cdc.infrastructure.persistence;

import com.tizitec.cdc.domain.model.Checkpoint;
import com.tizitec.cdc.domain.repository.CheckpointRepository;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class JdbcCheckpointRepository implements CheckpointRepository {

    // Defense-in-depth: the MERGE upsert should keep CDC_CHECKPOINT at exactly one row,
    // but if an operator inserts a row by hand during an incident we still want to
    // deterministically return the most recently written value.
    private static final String SELECT_LATEST =
        "SELECT LAST_SCN FROM CDC_CHECKPOINT "
        + "ORDER BY UPDATED_AT DESC, LAST_SCN DESC "
        + "FETCH FIRST 1 ROWS ONLY";

    // Monotonic upsert: the UPDATE branch only fires when the incoming SCN is strictly greater
    // than the stored one. Prevents multi-instance checkpoint regression (instance A committing
    // 1500 cannot be clobbered by a slower instance B committing 1400).
    private static final String UPSERT =
        "MERGE INTO CDC_CHECKPOINT t "
        + "USING (SELECT 1 FROM DUAL) src "
        + "ON (1 = 1) "
        + "WHEN MATCHED THEN UPDATE SET t.LAST_SCN = :lastScn, t.UPDATED_AT = SYSTIMESTAMP "
        + "  WHERE :lastScn > t.LAST_SCN "
        + "WHEN NOT MATCHED THEN INSERT (LAST_SCN, UPDATED_AT) VALUES (:lastScn, SYSTIMESTAMP)";

    private final NamedParameterJdbcTemplate jdbc;

    public JdbcCheckpointRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<Checkpoint> findLatest() {
        try {
            Long scn = jdbc.queryForObject(SELECT_LATEST, new MapSqlParameterSource(), Long.class);
            return scn == null ? Optional.empty() : Optional.of(new Checkpoint(scn));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public void save(Checkpoint checkpoint) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("lastScn", checkpoint.lastScn());
        jdbc.update(UPSERT, params);
    }
}
