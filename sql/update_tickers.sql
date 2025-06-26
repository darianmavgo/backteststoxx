-- Enable foreign key constraints and WAL mode
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

BEGIN TRANSACTION;

-- First, clear out any problematic tickers
UPDATE trade_signals
SET ticker = NULL;

-- Create a temporary table to store extracted tickers
CREATE TEMP TABLE IF NOT EXISTS temp_tickers AS
WITH email_content AS (
    -- Get plain_text content for searching
    SELECT 
        e.id as email_id,
        COALESCE(e.plain_text, '') as email_text
    FROM emails e
    JOIN trade_signals ts ON e.id = ts.email_id
),
extracted_tickers AS (
    -- Extract tickers using exchange format pattern
    SELECT 
        email_id,
        email_text,
        -- Match format: "Company Name (Exchange: TICKER)"
        CASE 
            -- Nasdaq format - strict uppercase match after colon
            WHEN UPPER(email_text) REGEXP '\( *NASDAQ: *[A-Z][A-Z]+ *\)'
            THEN TRIM(SUBSTR(
                SUBSTR(UPPER(email_text), INSTR(UPPER(email_text), 'NASDAQ:') + 7),
                1,
                INSTR(SUBSTR(UPPER(email_text), INSTR(UPPER(email_text), 'NASDAQ:') + 7), ')') - 1
            ))
            -- NYSE format - strict uppercase match after colon
            WHEN UPPER(email_text) REGEXP '\( *NYSE: *[A-Z][A-Z]+ *\)'
            THEN TRIM(SUBSTR(
                SUBSTR(UPPER(email_text), INSTR(UPPER(email_text), 'NYSE:') + 5),
                1,
                INSTR(SUBSTR(UPPER(email_text), INSTR(UPPER(email_text), 'NYSE:') + 5), ')') - 1
            ))
        END as ticker
    FROM email_content
),
valid_tickers AS (
    -- Filter out invalid tickers with stricter validation
    SELECT 
        email_id,
        ticker
    FROM extracted_tickers
    WHERE ticker IS NOT NULL
        -- Must be 2-5 uppercase letters
        AND LENGTH(ticker) BETWEEN 2 AND 5
        -- Must contain only uppercase letters
        AND ticker NOT REGEXP '[^A-Z]'
        -- Must not be common words or abbreviations
        AND ticker NOT IN (
            -- Common words to exclude
            'A', 'I', 'AT', 'BE', 'DO', 'GO', 'IF', 'IN', 'IS', 'IT', 'NO', 'OF', 'ON', 'OR', 
            'RE', 'SO', 'TO', 'UP', 'US', 'WE', 'PM', 'AM', 'EST', 'PST', 'GMT', 'UTC',
            'NEW', 'TOP', 'BUY', 'SELL', 'STOP', 'TAKE', 'PUT', 'CALL', 'THE', 'ALL',
            'ALERT', 'TRADE', 'STOCK', 'PRICE', 'HIGH', 'LOW', 'OPEN', 'CLOSE', 'FREE',
            'AND', 'FOR', 'FROM', 'INTO', 'NEXT', 'OUT', 'OVER', 'THIS', 'WITH', 'NEWS',
            'CEO', 'CFO', 'CTO', 'COO', 'IPO', 'ICO', 'ETF', 'ADR', 'NYSE', 'DJIA',
            'PICK', 'UPDATE', 'WEEKLY', 'TRIAL', 'SAVE'
        )
        -- Additional validation: must be preceded by exchange identifier with exact pattern
        AND (
            UPPER(email_text) REGEXP '\( *NASDAQ: *' || ticker || ' *\)'
            OR UPPER(email_text) REGEXP '\( *NYSE: *' || ticker || ' *\)'
        )
)
SELECT DISTINCT
    email_id,
    ticker
FROM valid_tickers;

-- Update trade_signals table with extracted tickers
UPDATE trade_signals
SET ticker = (
    SELECT ticker 
    FROM temp_tickers 
    WHERE temp_tickers.email_id = trade_signals.email_id
)
WHERE EXISTS (
    SELECT 1 
    FROM temp_tickers 
    WHERE temp_tickers.email_id = trade_signals.email_id
);

-- Drop existing view if it exists
DROP VIEW IF EXISTS v_ticker_extraction_results;

-- Create view for trade signals with snippets
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

-- Show statistics about ticker extraction
SELECT 
    COUNT(*) as total_signals,
    SUM(CASE WHEN ticker IS NOT NULL THEN 1 ELSE 0 END) as signals_with_tickers,
    ROUND(CAST(SUM(CASE WHEN ticker IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100, 2) as percentage_with_tickers
FROM trade_signals;

-- Show sample of extracted tickers to verify
SELECT DISTINCT
    ticker,
    COUNT(*) as count,
    SUBSTR(MAX(plain_text), 1, 200) as sample_text
FROM trade_signals ts
JOIN emails e ON ts.email_id = e.id
WHERE ts.ticker IS NOT NULL
GROUP BY ticker
ORDER BY count DESC
LIMIT 10;

COMMIT;

-- Example queries to verify results:
/*
-- View potentially problematic extractions (short tickers or common words)
SELECT 
    ticker,
    COUNT(*) as count,
    MAX(snippet) as sample_text
FROM trade_signals ts
JOIN emails e ON ts.email_id = e.id
WHERE ts.ticker IS NOT NULL
GROUP BY ticker
HAVING LENGTH(ticker) < 3
    OR ticker IN (
        SELECT word 
        FROM (
            SELECT 'FREE' as word
            UNION SELECT 'NEWS'
            UNION SELECT 'STOCK'
            -- Add more suspicious words here
        )
    )
ORDER BY count DESC;

-- View all unique tickers and their frequencies
SELECT 
    ticker,
    COUNT(*) as count,
    MIN(datetime(signal_date, 'unixepoch')) as first_seen,
    MAX(datetime(signal_date, 'unixepoch')) as last_seen,
    MAX(snippet) as sample_text
FROM trade_signals ts
JOIN emails e ON ts.email_id = e.id
WHERE ticker IS NOT NULL
GROUP BY ticker
ORDER BY count DESC;
*/ 