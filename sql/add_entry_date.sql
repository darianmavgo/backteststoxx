-- Enable foreign key constraints and WAL mode
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

BEGIN TRANSACTION;

-- Create a new table with the entry_date column
CREATE TABLE trade_signals_new (
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

-- Copy data to the new table, setting entry_date to signal_date + 1 day (86400000 milliseconds)
INSERT INTO trade_signals_new (
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
    signal_date + 86400000,  -- Add one day in milliseconds
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

-- Show sample of dates to verify
SELECT 
    ticker,
    datetime(signal_date/1000, 'unixepoch') as signal_datetime,
    datetime(entry_date/1000, 'unixepoch') as entry_datetime,
    buy_price,
    stop_price,
    target_price
FROM trade_signals
WHERE ticker IS NOT NULL
ORDER BY signal_date DESC
LIMIT 5;

COMMIT; 