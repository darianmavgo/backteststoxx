-- Enable foreign key constraints and WAL mode
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

BEGIN TRANSACTION;

-- First, copy any non-NULL values from stop_loss to stop_price
UPDATE trade_signals
SET stop_price = stop_loss
WHERE stop_loss IS NOT NULL
  AND stop_price IS NULL;

-- Create a new table without the stop_loss column
CREATE TABLE trade_signals_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id TEXT NOT NULL UNIQUE,
    ticker TEXT,
    signal_date INTEGER,
    buy_price REAL,
    stop_price DECIMAL,
    target_price REAL,
    FOREIGN KEY (email_id) REFERENCES emails(id)
);

-- Copy data to the new table
INSERT INTO trade_signals_new (
    id,
    email_id,
    ticker,
    signal_date,
    buy_price,
    stop_price,
    target_price
)
SELECT 
    id,
    email_id,
    ticker,
    signal_date,
    buy_price,
    stop_price,
    target_price
FROM trade_signals;

-- Drop the old table
DROP TABLE trade_signals;

-- Rename the new table
ALTER TABLE trade_signals_new RENAME TO trade_signals;

-- Recreate the unique index
CREATE UNIQUE INDEX idx_trade_signals_email_id ON trade_signals(email_id);

-- Show statistics about stop prices
SELECT 
    COUNT(*) as total_signals,
    SUM(CASE WHEN stop_price IS NOT NULL THEN 1 ELSE 0 END) as signals_with_stop_price,
    ROUND(CAST(SUM(CASE WHEN stop_price IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / 
          CAST(COUNT(*) AS FLOAT) * 100, 2) as stop_price_coverage
FROM trade_signals;

COMMIT; 