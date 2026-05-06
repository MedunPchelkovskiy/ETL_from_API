CREATE TABLE processing_state (
    processing_level   TEXT        NOT NULL,
    partition_date     DATE        NOT NULL,
    status             TEXT        NOT NULL,   -- pending / processing / success / failed / abandoned
    expected_count     INT,
    actual_count       INT,
    completeness_ratio FLOAT,
    is_acceptable      BOOLEAN,
    retry_count        INT         NOT NULL DEFAULT 0,
    created_at         TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMP   NOT NULL DEFAULT NOW(),
    error_type         TEXT,
    error_message      TEXT,
    PRIMARY KEY (processing_level, partition_date)
);

-- Optional: index for get_pending_work query pattern
CREATE INDEX idx_processing_state_pending
    ON processing_state (processing_level, status, retry_count);