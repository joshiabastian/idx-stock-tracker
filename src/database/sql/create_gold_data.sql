CREATE TABLE IF NOT EXISTS gold_data (
    date                DATE NOT NULL,
    ticker              VARCHAR(10) NOT NULL,
    ma20                NUMERIC(12, 4),
    ma50                NUMERIC(12, 4),
    ma100               NUMERIC(12, 4),
    rsi                 NUMERIC(8, 4),
    macd                NUMERIC(12, 4),
    macd_signal         NUMERIC(12, 4),
    relative_volume     NUMERIC(8, 4),
    score               NUMERIC(5, 4),
    PRIMARY KEY (date, ticker)
);