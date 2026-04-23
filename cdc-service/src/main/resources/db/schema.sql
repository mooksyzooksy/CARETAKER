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

-- STATUS alone is the hot filter for findAllNew; SCN + ROW_SEQUENCE is the
-- sort key. A covering composite index lets the FOR UPDATE SKIP LOCKED scan
-- walk the index in order without touching the heap until rows are picked.
CREATE INDEX CDC_INBOX_STATUS_ORDER_IDX
    ON CDC_INBOX(STATUS, SCN, ROW_SEQUENCE);

CREATE TABLE CDC_CHECKPOINT (
    LAST_SCN    NUMBER          NOT NULL,
    UPDATED_AT  TIMESTAMP       NOT NULL
);
