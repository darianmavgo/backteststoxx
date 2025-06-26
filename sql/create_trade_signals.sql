-- Enable foreign key constraints and WAL mode
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

BEGIN TRANSACTION;

-- Get statistics before changes
CREATE TEMP TABLE signal_stats AS
SELECT 
    COUNT(*) as total_signals,
    COUNT(DISTINCT email_id) as unique_emails
FROM trade_signals;

-- Backup existing data
CREATE TEMP TABLE temp_signals AS
SELECT DISTINCT email_id, 
       ticker,
       signal_date,
       buy_price,
       stop_loss,
       target_price
FROM trade_signals;

-- Drop existing view
DROP VIEW IF EXISTS v_ticker_extraction_results;

-- Drop existing table
DROP TABLE IF EXISTS trade_signals;

-- Recreate table with unique constraint
CREATE TABLE trade_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id TEXT NOT NULL UNIQUE,
    ticker TEXT,
    signal_date INTEGER,
    buy_price REAL,
    stop_loss REAL,
    target_price REAL,
    FOREIGN KEY (email_id) REFERENCES emails(id)
);

-- Create index on email_id
CREATE UNIQUE INDEX idx_trade_signals_email_id ON trade_signals(email_id);

-- Restore data (taking the first signal for each email)
INSERT INTO trade_signals (email_id, ticker, signal_date, buy_price, stop_loss, target_price)
SELECT email_id, ticker, signal_date, buy_price, stop_loss, target_price
FROM temp_signals;

-- Recreate the view
CREATE VIEW v_ticker_extraction_results AS
SELECT 
    ts.id as signal_id,
    ts.email_id,
    ts.ticker,
    e.plain_text as email_text,
    ts.signal_date,
    datetime(ts.signal_date, 'unixepoch') as signal_date_readable,
    ts.buy_price,
    ts.stop_loss,
    ts.target_price
FROM trade_signals ts
JOIN emails e ON ts.email_id = e.id
WHERE ts.ticker IS NOT NULL
ORDER BY ts.signal_date DESC;

-- Show deduplication statistics
SELECT 
    'Before deduplication' as stage,
    total_signals,
    unique_emails
FROM signal_stats
UNION ALL
SELECT 
    'After deduplication' as stage,
    COUNT(*) as total_signals,
    COUNT(DISTINCT email_id) as unique_emails
FROM trade_signals;

-- Show sample of current signals
SELECT 
    ticker,
    COUNT(*) as count,
    SUBSTR(MAX(plain_text), 1, 200) as sample_text
FROM trade_signals ts
JOIN emails e ON ts.email_id = e.id
WHERE ts.ticker IS NOT NULL
GROUP BY ticker
ORDER BY count DESC
LIMIT 5;

-- Cleanup
DROP TABLE temp_signals;
DROP TABLE signal_stats;

COMMIT;

-- Drop existing views if they exist
DROP VIEW IF EXISTS v_trade_signals;
DROP VIEW IF EXISTS v_trade_signal_stats;

-- Create view for trade signals with email details
CREATE VIEW v_trade_signals AS
SELECT 
    ts.id as signal_id,
    ts.email_id,
    ts.ticker,
    ts.signal_date,
    datetime(ts.signal_date, 'unixepoch') as signal_date_readable,
    ts.buy_price,
    ts.stop_loss,
    ts.target_price,
    e.subject as email_subject,
    e.from_address as sender,
    e.plain_text as email_content,
    datetime(ts.created_at, 'unixepoch') as created_at_readable,
    datetime(ts.updated_at, 'unixepoch') as updated_at_readable
FROM trade_signals ts
JOIN emails e ON ts.email_id = e.id;

-- Create view for trade signal statistics
CREATE VIEW v_trade_signal_stats AS
SELECT 
    ticker,
    COUNT(*) as total_signals,
    COUNT(CASE WHEN buy_price IS NOT NULL THEN 1 END) as signals_with_prices,
    MIN(signal_date) as oldest_signal_date,
    MAX(signal_date) as newest_signal_date,
    AVG(CASE 
        WHEN target_price IS NOT NULL AND buy_price IS NOT NULL 
        THEN ((target_price - buy_price) / buy_price) * 100 
        END) as avg_target_percent,
    AVG(CASE 
        WHEN stop_loss IS NOT NULL AND buy_price IS NOT NULL 
        THEN ((stop_loss - buy_price) / buy_price) * 100 
        END) as avg_stop_percent
FROM trade_signals
WHERE ticker IS NOT NULL
GROUP BY ticker;

-- Example queries:
/*
-- Get all trade signals for a specific ticker and date range
SELECT * FROM v_trade_signals 
WHERE ticker = 'AAPL'
    AND signal_date BETWEEN strftime('%s', '2024-01-01') AND strftime('%s', '2024-12-31')
ORDER BY signal_date DESC;

-- Get trade signals with complete price information
SELECT * FROM v_trade_signals 
WHERE ticker IS NOT NULL
    AND buy_price IS NOT NULL 
    AND stop_loss IS NOT NULL 
    AND target_price IS NOT NULL
ORDER BY signal_date DESC;

-- Get trade signal statistics by ticker and month
SELECT 
    ticker,
    strftime('%Y-%m', datetime(signal_date, 'unixepoch')) as month,
    COUNT(*) as signal_count,
    AVG(CASE 
        WHEN target_price IS NOT NULL AND buy_price IS NOT NULL 
        THEN ((target_price - buy_price) / buy_price) * 100 
        END) as avg_target_percent
FROM trade_signals
WHERE ticker IS NOT NULL
GROUP BY ticker, month
ORDER BY ticker, month DESC;

-- Get most active tickers
SELECT 
    ticker,
    COUNT(*) as signal_count,
    MIN(signal_date) as first_signal,
    MAX(signal_date) as last_signal
FROM trade_signals
WHERE ticker IS NOT NULL
GROUP BY ticker
ORDER BY signal_count DESC;
*/ 