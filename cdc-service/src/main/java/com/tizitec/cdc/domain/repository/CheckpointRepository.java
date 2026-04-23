package com.tizitec.cdc.domain.repository;

import com.tizitec.cdc.domain.model.Checkpoint;

import java.util.Optional;

public interface CheckpointRepository {

    Optional<Checkpoint> findLatest();

    void save(Checkpoint checkpoint);
}
