CREATE VIEW close_trade_signals AS
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
        LAG(signal_date) OVER (ORDER BY signal_date) AS prev_signal_date
    FROM trade_signals
) AS t
WHERE prev_signal_date IS NULL 
   OR (signal_date - prev_signal_date) < 259200000;