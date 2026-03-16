CREATE TABLE IF NOT EXISTS raw_data (
    date        DATE NOT NULL,
    ticker      VARCHAR(10) NOT NULL,
    open        NUMERIC(12, 4),
    high        NUMERIC(12, 4),
    low         NUMERIC(12, 4),
    close       NUMERIC(12, 4),
    adj_close   NUMERIC(12, 4),
    volume      BIGINT,
    PRIMARY KEY (date, ticker)
);