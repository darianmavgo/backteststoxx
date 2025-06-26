-- Enable foreign key constraints and WAL mode
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -2000;

-- Start fresh
DROP TABLE IF EXISTS temp_prices;

BEGIN IMMEDIATE TRANSACTION;

-- Create a temporary table to store extracted prices
CREATE TEMP TABLE temp_prices AS
WITH valid_emails AS (
    -- Get emails with sufficient plain_text content and valid tickers
    SELECT 
        e.id as email_id,
        ts.ticker,
        UPPER(TRIM(COALESCE(e.plain_text, ''))) as email_text
    FROM emails e
    JOIN trade_signals ts ON e.id = ts.email_id
    WHERE LENGTH(TRIM(COALESCE(e.plain_text, ''))) > 20
      AND ts.ticker IS NOT NULL
),
price_positions AS (
    SELECT 
        email_id,
        ticker,
        email_text,
        -- Find positions of key words
        INSTR(email_text, 'BUY') as buy_pos,
        INSTR(email_text, 'STOP') as stop_pos,
        INSTR(email_text, 'TARGET') as target_pos
    FROM valid_emails
    WHERE INSTR(email_text, 'BUY') > 0  -- Only process emails with BUY signal
),
number_positions AS (
    SELECT 
        email_id,
        ticker,
        email_text,
        buy_pos,
        stop_pos,
        target_pos,
        -- Extract text segments after keywords (larger context)
        SUBSTR(email_text, buy_pos, 100) as buy_segment,
        SUBSTR(email_text, stop_pos, 100) as stop_segment,
        SUBSTR(email_text, target_pos, 100) as target_segment
    FROM price_positions
),
extracted_numbers AS (
    SELECT 
        email_id,
        ticker,
        -- Extract first number after BUY
        CAST(
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                SUBSTR(
                                    buy_segment,
                                    CASE 
                                        WHEN INSTR(buy_segment, '@') > 0 
                                        THEN INSTR(buy_segment, '@') + 1
                                        WHEN INSTR(buy_segment, '$') > 0 
                                        THEN INSTR(buy_segment, '$') + 1
                                        WHEN INSTR(buy_segment, 'UNDER') > 0 
                                        THEN INSTR(buy_segment, 'UNDER') + 5
                                        WHEN INSTR(buy_segment, 'UP TO') > 0 
                                        THEN INSTR(buy_segment, 'UP TO') + 5
                                        WHEN INSTR(buy_segment, 'AT') > 0 
                                        THEN INSTR(buy_segment, 'AT') + 2
                                        ELSE INSTR(buy_segment, ' ') + 1
                                    END,
                                    20  -- Get enough characters to capture the number
                                ),
                                '$', ''
                            ),
                            '@', ''
                        ),
                        ' ', ''
                    ),
                    ',', ''
                )
            ) AS DECIMAL
        ) as buy_price,
        -- Extract first number after STOP
        CAST(
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                SUBSTR(
                                    stop_segment,
                                    CASE 
                                        WHEN INSTR(stop_segment, '@') > 0 
                                        THEN INSTR(stop_segment, '@') + 1
                                        WHEN INSTR(stop_segment, '$') > 0 
                                        THEN INSTR(stop_segment, '$') + 1
                                        WHEN INSTR(stop_segment, 'AT') > 0 
                                        THEN INSTR(stop_segment, 'AT') + 2
                                        ELSE INSTR(stop_segment, ' ') + 1
                                    END,
                                    20  -- Get enough characters to capture the number
                                ),
                                '$', ''
                            ),
                            '@', ''
                        ),
                        ' ', ''
                    ),
                    ',', ''
                )
            ) AS DECIMAL
        ) as stop_price,
        -- Extract first number after TARGET
        CAST(
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                SUBSTR(
                                    target_segment,
                                    CASE 
                                        WHEN INSTR(target_segment, '@') > 0 
                                        THEN INSTR(target_segment, '@') + 1
                                        WHEN INSTR(target_segment, '$') > 0 
                                        THEN INSTR(target_segment, '$') + 1
                                        WHEN INSTR(target_segment, 'AT') > 0 
                                        THEN INSTR(target_segment, 'AT') + 2
                                        ELSE INSTR(target_segment, ' ') + 1
                                    END,
                                    20  -- Get enough characters to capture the number
                                ),
                                '$', ''
                            ),
                            '@', ''
                        ),
                        ' ', ''
                    ),
                    ',', ''
                )
            ) AS DECIMAL
        ) as target_price
    FROM number_positions
),
validated_prices AS (
    -- Apply validation rules to extracted prices
    SELECT 
        email_id,
        ticker,
        buy_price,
        stop_price,
        target_price
    FROM extracted_numbers
    WHERE 
        -- Ensure prices are positive and within reasonable range
        buy_price > 0 AND buy_price < 10000
        AND stop_price > 0 AND stop_price < 10000
        AND target_price > 0 AND target_price < 10000
        -- Basic price relationship validation (with some tolerance)
        AND target_price >= buy_price * 0.9  -- Allow 10% tolerance
        AND buy_price >= stop_price * 0.9    -- Allow 10% tolerance
)
SELECT DISTINCT * FROM validated_prices;

-- Update trade_signals table with extracted prices
UPDATE trade_signals
SET 
    buy_price = (
        SELECT buy_price 
        FROM temp_prices 
        WHERE temp_prices.email_id = trade_signals.email_id
        AND temp_prices.ticker = trade_signals.ticker
    ),
    stop_price = (
        SELECT stop_price
        FROM temp_prices 
        WHERE temp_prices.email_id = trade_signals.email_id
        AND temp_prices.ticker = trade_signals.ticker
    ),
    target_price = (
        SELECT target_price
        FROM temp_prices 
        WHERE temp_prices.email_id = trade_signals.email_id
        AND temp_prices.ticker = trade_signals.ticker
    )
WHERE EXISTS (
    SELECT 1 
    FROM temp_prices 
    WHERE temp_prices.email_id = trade_signals.email_id
    AND temp_prices.ticker = trade_signals.ticker
);

-- Show statistics about price extraction
SELECT 
    COUNT(*) as total_signals,
    SUM(CASE WHEN ticker IS NOT NULL THEN 1 ELSE 0 END) as signals_with_tickers,
    SUM(CASE WHEN buy_price IS NOT NULL THEN 1 ELSE 0 END) as signals_with_buy_price,
    SUM(CASE WHEN stop_price IS NOT NULL THEN 1 ELSE 0 END) as signals_with_stop_price,
    SUM(CASE WHEN target_price IS NOT NULL THEN 1 ELSE 0 END) as signals_with_target_price,
    SUM(CASE WHEN buy_price IS NOT NULL AND stop_price IS NOT NULL AND target_price IS NOT NULL THEN 1 ELSE 0 END) as complete_signals,
    ROUND(CAST(SUM(CASE WHEN buy_price IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / 
          CAST(SUM(CASE WHEN ticker IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) * 100, 2) as buy_price_coverage,
    ROUND(CAST(SUM(CASE WHEN buy_price IS NOT NULL AND stop_price IS NOT NULL AND target_price IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / 
          CAST(SUM(CASE WHEN ticker IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) * 100, 2) as complete_signal_coverage
FROM trade_signals
WHERE ticker IS NOT NULL;

-- Show sample of successfully extracted prices
SELECT 
    ticker,
    buy_price,
    stop_price,
    target_price,
    SUBSTR(e.plain_text, 1, 200) as sample_text
FROM trade_signals ts
JOIN emails e ON ts.email_id = e.id
WHERE ts.ticker IS NOT NULL
  AND ts.buy_price IS NOT NULL
  AND ts.stop_price IS NOT NULL
  AND ts.target_price IS NOT NULL
ORDER BY ts.signal_date DESC
LIMIT 5;

-- Show sample of missed prices
SELECT 
    ticker,
    buy_price,
    stop_price,
    target_price,
    SUBSTR(e.plain_text, 1, 200) as sample_text
FROM trade_signals ts
JOIN emails e ON ts.email_id = e.id
WHERE ts.ticker IS NOT NULL
  AND (ts.buy_price IS NULL OR ts.stop_price IS NULL OR ts.target_price IS NULL)
ORDER BY ts.signal_date DESC
LIMIT 5;

COMMIT;

-- Vacuum to clean up
VACUUM; 