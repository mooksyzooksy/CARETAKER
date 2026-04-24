CREATE SEQUENCE CDC_INBOX_SEQ
    START WITH 1
    INCREMENT BY 1
    CACHE 20
    NOCYCLE;

CREATE TABLE CDC_INBOX (
    ID            NUMBER PRIMARY KEY,
    TABLE_NAME    VARCHAR2(100)   NOT NULL,
    OPERATION     VARCHAR2(10)    NOT NULL,
    PAYLOAD       CLOB            NOT NULL,
    SCN           NUMBER          NOT NULL,
    ROW_SEQUENCE  NUMBER          DEFAULT 0 NOT NULL,
    STATUS        VARCHAR2(10)    DEFAULT 'NEW' NOT NULL,
    RETRY_COUNT   NUMBER          DEFAULT 0 NOT NULL,
    CAPTURED_AT   TIMESTAMP       NOT NULL,
    UPDATED_AT    TIMESTAMP       NOT NULL
);

-- Prevents duplicate inbox rows on capture retry (e.g. crash after DB write but
-- before in-memory SCN advance). The triple (TABLE_NAME, SCN, ROW_SEQUENCE) maps
-- 1-to-1 with a unique Oracle change event: SCN is monotonic per database, and
-- ROW_SEQUENCE is the intra-SCN position assigned by the LogMiner library.
-- Without this constraint a re-mined range silently inserts phantom duplicates
-- that would later be relayed as duplicate Kafka messages.
CREATE UNIQUE INDEX CDC_INBOX_DEDUP_IDX
    ON CDC_INBOX(TABLE_NAME, SCN, ROW_SEQUENCE);

-- STATUS alone is the hot filter for findAllNew; SCN + ROW_SEQUENCE is the
-- sort key. A covering composite index lets the FOR UPDATE SKIP LOCKED scan
-- walk the index in order without touching the heap until rows are picked.
CREATE INDEX CDC_INBOX_STATUS_ORDER_IDX
    ON CDC_INBOX(STATUS, SCN, ROW_SEQUENCE);

CREATE TABLE CDC_CHECKPOINT (
    LAST_SCN    NUMBER          NOT NULL,
    UPDATED_AT  TIMESTAMP       NOT NULL
);
