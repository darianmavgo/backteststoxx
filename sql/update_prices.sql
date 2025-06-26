-- Enable foreign key constraints and WAL mode
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

BEGIN TRANSACTION;

-- Create a temporary table to store extracted prices
CREATE TEMP TABLE IF NOT EXISTS temp_prices AS
WITH valid_emails AS (
    -- Get emails with sufficient plain_text content
    SELECT 
        e.id as email_id,
        ts.ticker,
        UPPER(TRIM(COALESCE(e.plain_text, ''))) as email_text
    FROM emails e
    JOIN trade_signals ts ON e.id = ts.email_id
    WHERE LENGTH(TRIM(COALESCE(e.plain_text, ''))) > 20
      AND ts.ticker IS NOT NULL
),
extracted_prices AS (
    SELECT 
        email_id,
        ticker,
        -- Extract buy price using various patterns
        CASE 
            -- Match "Buy at X" or "Buy @ X"
            WHEN email_text REGEXP 'BUY AT [$]?[0-9]+[.]?[0-9]*' 
            THEN CAST(TRIM(SUBSTR(
                SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY AT ') + 7),
                CASE WHEN SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY AT ') + 7, 1) = '$' THEN 2 ELSE 1 END,
                INSTR(SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY AT ') + 7), ' ') - 1
            )) AS DECIMAL)
            -- Match "Buy under X" or "Buy below X"
            WHEN email_text REGEXP 'BUY UNDER [$]?[0-9]+[.]?[0-9]*|BUY BELOW [$]?[0-9]+[.]?[0-9]*'
            THEN CASE 
                WHEN INSTR(email_text, 'BUY UNDER') > 0 
                THEN CAST(TRIM(SUBSTR(
                    SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY UNDER ') + 10),
                    CASE WHEN SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY UNDER ') + 10, 1) = '$' THEN 2 ELSE 1 END,
                    INSTR(SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY UNDER ') + 10), ' ') - 1
                )) AS DECIMAL)
                ELSE CAST(TRIM(SUBSTR(
                    SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY BELOW ') + 10),
                    CASE WHEN SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY BELOW ') + 10, 1) = '$' THEN 2 ELSE 1 END,
                    INSTR(SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY BELOW ') + 10), ' ') - 1
                )) AS DECIMAL)
            END
            -- Match "Buy up to X"
            WHEN email_text REGEXP 'BUY UP TO [$]?[0-9]+[.]?[0-9]*'
            THEN CAST(TRIM(SUBSTR(
                SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY UP TO ') + 10),
                CASE WHEN SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY UP TO ') + 10, 1) = '$' THEN 2 ELSE 1 END,
                INSTR(SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY UP TO ') + 10), ' ') - 1
            )) AS DECIMAL)
            -- Match "BUY $X" format
            WHEN email_text REGEXP 'BUY [$][0-9]+[.]?[0-9]*'
            THEN CAST(TRIM(SUBSTR(
                SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY $') + 5),
                1,
                INSTR(SUBSTR(email_text, INSTR(UPPER(email_text), 'BUY $') + 5), ' ') - 1
            )) AS DECIMAL)
        END as buy_price,
        
        -- Extract stop loss price
        CASE 
            -- Match "Stop X" pattern
            WHEN INSTR(email_text, 'STOP ') > 0
            THEN CAST(TRIM(SUBSTR(
                SUBSTR(email_text, INSTR(email_text, 'STOP ') + 5),
                CASE WHEN SUBSTR(email_text, INSTR(email_text, 'STOP ') + 5, 1) = '$' THEN 2 ELSE 1 END,
                INSTR(SUBSTR(email_text, INSTR(email_text, 'STOP ') + 5), ' ') - 1
            )) AS DECIMAL)
            -- Match "Stop-loss X" pattern
            WHEN INSTR(email_text, 'STOP-LOSS ') > 0
            THEN CAST(TRIM(SUBSTR(
                SUBSTR(email_text, INSTR(email_text, 'STOP-LOSS ') + 10),
                CASE WHEN SUBSTR(email_text, INSTR(email_text, 'STOP-LOSS ') + 10, 1) = '$' THEN 2 ELSE 1 END,
                INSTR(SUBSTR(email_text, INSTR(email_text, 'STOP-LOSS ') + 10), ' ') - 1
            )) AS DECIMAL)
            -- Match "Stop at X" pattern
            WHEN INSTR(email_text, 'STOP AT ') > 0
            THEN CAST(TRIM(SUBSTR(
                SUBSTR(email_text, INSTR(email_text, 'STOP AT ') + 8),
                CASE WHEN SUBSTR(email_text, INSTR(email_text, 'STOP AT ') + 8, 1) = '$' THEN 2 ELSE 1 END,
                INSTR(SUBSTR(email_text, INSTR(email_text, 'STOP AT ') + 8), ' ') - 1
            )) AS DECIMAL)
        END as stop_loss,
        
        -- Extract target price
        CASE 
            -- Match "Target X" pattern
            WHEN INSTR(email_text, 'TARGET ') > 0
            THEN CAST(TRIM(SUBSTR(
                SUBSTR(email_text, INSTR(email_text, 'TARGET ') + 7),
                CASE WHEN SUBSTR(email_text, INSTR(email_text, 'TARGET ') + 7, 1) = '$' THEN 2 ELSE 1 END,
                INSTR(SUBSTR(email_text, INSTR(email_text, 'TARGET ') + 7), ' ') - 1
            )) AS DECIMAL)
            -- Match "Target at X" pattern
            WHEN INSTR(email_text, 'TARGET AT ') > 0
            THEN CAST(TRIM(SUBSTR(
                SUBSTR(email_text, INSTR(email_text, 'TARGET AT ') + 10),
                CASE WHEN SUBSTR(email_text, INSTR(email_text, 'TARGET AT ') + 10, 1) = '$' THEN 2 ELSE 1 END,
                INSTR(SUBSTR(email_text, INSTR(email_text, 'TARGET AT ') + 10), ' ') - 1
            )) AS DECIMAL)
        END as target_price
    FROM valid_emails
),
validated_prices AS (
    -- Apply validation rules to extracted prices
    SELECT 
        email_id,
        ticker,
        buy_price,
        stop_loss,
        target_price
    FROM extracted_prices
    WHERE 
        -- Ensure prices are positive
        (buy_price IS NULL OR buy_price > 0)
        AND (stop_loss IS NULL OR stop_loss > 0)
        AND (target_price IS NULL OR target_price > 0)
        -- Ensure logical price relationships
        AND (
            (buy_price IS NULL OR stop_loss IS NULL OR buy_price > stop_loss)
            AND (buy_price IS NULL OR target_price IS NULL OR target_price > buy_price)
            AND (stop_loss IS NULL OR target_price IS NULL OR target_price > stop_loss)
        )
        -- Ensure prices are within reasonable ranges
        AND (buy_price IS NULL OR buy_price < 10000)
        AND (stop_loss IS NULL OR stop_loss < 10000)
        AND (target_price IS NULL OR target_price < 10000)
)
SELECT DISTINCT
    email_id,
    FIRST_VALUE(buy_price) OVER (PARTITION BY email_id ORDER BY 
        CASE 
            WHEN buy_price IS NOT NULL THEN 0 
            ELSE 1 
        END, 
        buy_price
    ) as buy_price,
    FIRST_VALUE(stop_loss) OVER (PARTITION BY email_id ORDER BY 
        CASE 
            WHEN stop_loss IS NOT NULL THEN 0 
            ELSE 1 
        END, 
        stop_loss
    ) as stop_loss,
    FIRST_VALUE(target_price) OVER (PARTITION BY email_id ORDER BY 
        CASE 
            WHEN target_price IS NOT NULL THEN 0 
            ELSE 1 
        END, 
        target_price
    ) as target_price
FROM validated_prices;

-- Update trade_signals table with extracted prices
UPDATE trade_signals
SET 
    buy_price = (
        SELECT buy_price 
        FROM temp_prices 
        WHERE temp_prices.email_id = trade_signals.email_id
    ),
    stop_loss = (
        SELECT stop_loss 
        FROM temp_prices 
        WHERE temp_prices.email_id = trade_signals.email_id
    ),
    target_price = (
        SELECT target_price 
        FROM temp_prices 
        WHERE temp_prices.email_id = trade_signals.email_id
    )
WHERE EXISTS (
    SELECT 1 
    FROM temp_prices 
    WHERE temp_prices.email_id = trade_signals.email_id
);

-- Create view for signals with risk metrics
DROP VIEW IF EXISTS v_trade_signals_with_risk;
CREATE VIEW v_trade_signals_with_risk AS
SELECT 
    ts.*,
    ROUND(CAST(target_price - buy_price AS FLOAT) / NULLIF(buy_price, 0) * 100, 2) as potential_gain_percent,
    ROUND(CAST(buy_price - stop_loss AS FLOAT) / NULLIF(buy_price, 0) * 100, 2) as max_loss_percent,
    ROUND(
        CAST(target_price - buy_price AS FLOAT) / NULLIF(buy_price, 0) * 100 /
        NULLIF(CAST(buy_price - stop_loss AS FLOAT) / NULLIF(buy_price, 0) * 100, 0),
        2
    ) as reward_risk_ratio
FROM trade_signals ts
WHERE ts.buy_price IS NOT NULL 
  AND ts.stop_loss IS NOT NULL 
  AND ts.target_price IS NOT NULL;

-- Show statistics about price extraction
SELECT 
    COUNT(*) as total_signals,
    SUM(CASE WHEN buy_price IS NOT NULL THEN 1 ELSE 0 END) as signals_with_buy_price,
    ROUND(CAST(SUM(CASE WHEN buy_price IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100, 2) as buy_price_percentage,
    SUM(CASE WHEN stop_loss IS NOT NULL THEN 1 ELSE 0 END) as signals_with_stop_loss,
    ROUND(CAST(SUM(CASE WHEN stop_loss IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100, 2) as stop_loss_percentage,
    SUM(CASE WHEN target_price IS NOT NULL THEN 1 ELSE 0 END) as signals_with_target,
    ROUND(CAST(SUM(CASE WHEN target_price IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100, 2) as target_percentage,
    SUM(CASE WHEN buy_price IS NOT NULL AND stop_loss IS NOT NULL AND target_price IS NOT NULL THEN 1 ELSE 0 END) as complete_signals,
    ROUND(CAST(SUM(CASE WHEN buy_price IS NOT NULL AND stop_loss IS NOT NULL AND target_price IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100, 2) as complete_percentage
FROM trade_signals;

-- Show sample of extracted prices
SELECT 
    ticker,
    buy_price,
    stop_loss,
    target_price,
    SUBSTR(e.plain_text, 1, 200) as sample_text
FROM trade_signals ts
JOIN emails e ON ts.email_id = e.id
WHERE ts.buy_price IS NOT NULL 
   OR ts.stop_loss IS NOT NULL 
   OR ts.target_price IS NOT NULL
ORDER BY ts.signal_date DESC
LIMIT 5;

COMMIT;

-- Example queries to verify results:
/*
-- View all signals with complete price information
SELECT * FROM v_trade_signals_with_risk;

-- Find signals with unusually high reward/risk ratios
SELECT *
FROM v_trade_signals_with_risk
WHERE reward_risk_ratio > 3
ORDER BY reward_risk_ratio DESC;

-- View average risk metrics by ticker
SELECT 
    ticker,
    COUNT(*) as signal_count,
    ROUND(AVG(potential_gain_percent), 2) as avg_potential_gain,
    ROUND(AVG(max_loss_percent), 2) as avg_max_loss,
    ROUND(AVG(reward_risk_ratio), 2) as avg_reward_risk_ratio
FROM v_trade_signals_with_risk
GROUP BY ticker
HAVING signal_count > 1
ORDER BY signal_count DESC;

-- Test specific pattern
SELECT 
    CAST(TRIM(SUBSTR(
        'Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220',
        INSTR(LOWER('Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220'), 'buy at ') + 7,
        INSTR(SUBSTR('Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220', 
               INSTR(LOWER('Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220'), 'buy at ') + 7)||' ', ' ') - 1
    )) AS DECIMAL(10,2)) as buy_price,
    CAST(TRIM(SUBSTR(
        'Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220',
        INSTR(LOWER('Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220'), 'stop ') + 5,
        INSTR(SUBSTR('Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220', 
               INSTR(LOWER('Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220'), 'stop ') + 5)||' ', ' ') - 1
    )) AS DECIMAL(10,2)) as stop_loss,
    CAST(TRIM(SUBSTR(
        'Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220',
        INSTR(LOWER('Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220'), 'target ') + 7,
        INSTR(SUBSTR('Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220', 
               INSTR(LOWER('Arista Networks (Nasdaq: ANET) Buy at 188.6 Stop 179 Target 220'), 'target ') + 7)||' ', ' ') - 1
    )) AS DECIMAL(10,2)) as target_price;
*/ 