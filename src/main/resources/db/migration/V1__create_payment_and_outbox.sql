-- Payment aggregate. A payment is completed in the same transaction that
-- writes its outbox event, so the two can never diverge (the dual-write problem).
CREATE TABLE payment (
    id           VARCHAR(36)    NOT NULL,
    amount       DECIMAL(19, 4) NOT NULL,
    currency     VARCHAR(3)     NOT NULL,
    status       VARCHAR(20)    NOT NULL,
    created_at   DATETIME(6)    NOT NULL,
    completed_at DATETIME(6)    NULL,
    PRIMARY KEY (id)
);

-- Transactional outbox. Rows are inserted PENDING inside the business
-- transaction and flipped to SENT by the relay only after a confirmed publish.
CREATE TABLE outbox (
    id             VARCHAR(36)  NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id   VARCHAR(64) NOT NULL,
    event_type     VARCHAR(64) NOT NULL,
    payload        JSON        NOT NULL,
    status         VARCHAR(16) NOT NULL,
    created_at     DATETIME(6) NOT NULL,
    sent_at        DATETIME(6) NULL,
    PRIMARY KEY (id)
);

-- The relay polls PENDING rows oldest-first; this index keeps that query cheap.
CREATE INDEX idx_outbox_status_created ON outbox (status, created_at);
