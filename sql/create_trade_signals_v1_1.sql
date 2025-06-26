/*
 SQLite3 script to create trade_signals_v1_1 table, selecting non-null buy_price and non-null ticker records
 from trade_signals and deduplicating based on ticker, buy_price, stop_price, and target_price.
*/

PRAGMA foreign_keys = false;

-- Drop the table if it already exists
DROP TABLE IF EXISTS "trade_signals_v1_1";

-- Create the trade_signals_v1_1 table with the same structure as trade_signals
CREATE TABLE "trade_signals_v1_1" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id TEXT NOT NULL UNIQUE,
    ticker TEXT,
    signal_date INTEGER,
    entry_date INTEGER,
    buy_price REAL,
    stop_price DECIMAL,
    target_price REAL,
    FOREIGN KEY (email_id) REFERENCES emails(id)
);

-- Insert deduplicated records with non-null buy_price and non-null ticker
INSERT INTO "trade_signals_v1_1" (
    id,
    email_id,
    ticker,
    signal_date,
    entry_date,
    buy_price,
    stop_price,
    target_price
)
SELECT 
    id,
    email_id,
    ticker,
    signal_date,
    entry_date,
    buy_price,
    stop_price,
    target_price
FROM (
    SELECT 
        id,
        email_id,
        ticker,
        signal_date,
        entry_date,
        buy_price,
        stop_price,
        target_price,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, buy_price, stop_price, target_price 
            ORDER BY signal_date
        ) AS rn
    FROM trade_signals
    WHERE buy_price IS NOT NULL
      AND ticker IS NOT NULL
) t
WHERE rn = 1;

-- Create unique index on email_id
CREATE UNIQUE INDEX "idx_trade_signals_v1_1_email_id"
ON "trade_signals_v1_1" (
    "email_id" ASC
);

PRAGMA foreign_keys = true;