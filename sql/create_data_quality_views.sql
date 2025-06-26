-- Enable foreign key constraints and WAL mode
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

BEGIN TRANSACTION;

-- Drop existing views if they exist
DROP VIEW IF EXISTS v_email_data_quality;
DROP VIEW IF EXISTS v_email_data_quality_summary;
DROP VIEW IF EXISTS v_short_emails;
DROP VIEW IF EXISTS missing_signal_words;

-- Create view for individual email quality issues
CREATE VIEW v_email_data_quality AS
SELECT 
    id as email_id,
    date as email_date,
    datetime(date, 'unixepoch') as email_date_readable,
    subject,
    CASE 
        WHEN plain_text IS NULL AND html IS NULL THEN 'No content'
        WHEN plain_text IS NULL THEN 'Missing plain_text'
        WHEN html IS NULL THEN 'Missing html'
        WHEN LENGTH(TRIM(plain_text)) = 0 AND LENGTH(TRIM(html)) = 0 THEN 'Empty content'
        WHEN LENGTH(TRIM(plain_text)) = 0 THEN 'Empty plain_text'
        WHEN LENGTH(TRIM(html)) = 0 THEN 'Empty html'
        ELSE 'OK'
    END as content_status,
    LENGTH(COALESCE(plain_text, '')) as plain_text_length,
    LENGTH(COALESCE(html, '')) as html_length,
    COALESCE(snippet, '') as snippet_preview
FROM emails
WHERE plain_text IS NULL 
   OR html IS NULL 
   OR LENGTH(TRIM(COALESCE(plain_text, ''))) = 0 
   OR LENGTH(TRIM(COALESCE(html, ''))) = 0;

-- Create view for emails with very short plain_text
CREATE VIEW v_short_emails AS
SELECT 
    id as email_id,
    date as email_date,
    datetime(date, 'unixepoch') as email_date_readable,
    subject,
    LENGTH(TRIM(COALESCE(plain_text, ''))) as plain_text_length,
    LENGTH(COALESCE(html, '')) as html_length,
    COALESCE(plain_text, '') as plain_text_content,
    COALESCE(snippet, '') as snippet_preview
FROM emails
WHERE LENGTH(TRIM(COALESCE(plain_text, ''))) < 20
ORDER BY date DESC;

-- Create summary view for data quality statistics
CREATE VIEW v_email_data_quality_summary AS
WITH email_stats AS (
    SELECT 
        COUNT(*) as total_emails,
        SUM(CASE WHEN plain_text IS NULL AND html IS NULL THEN 1 ELSE 0 END) as no_content_count,
        SUM(CASE WHEN plain_text IS NULL AND html IS NOT NULL THEN 1 ELSE 0 END) as missing_plain_text_count,
        SUM(CASE WHEN html IS NULL AND plain_text IS NOT NULL THEN 1 ELSE 0 END) as missing_html_count,
        SUM(CASE 
            WHEN LENGTH(TRIM(COALESCE(plain_text, ''))) = 0 
             AND LENGTH(TRIM(COALESCE(html, ''))) = 0 
             AND (plain_text IS NOT NULL OR html IS NOT NULL)
            THEN 1 ELSE 0 END) as empty_content_count,
        SUM(CASE 
            WHEN LENGTH(TRIM(COALESCE(plain_text, ''))) = 0 
             AND LENGTH(TRIM(COALESCE(html, ''))) > 0 
             AND plain_text IS NOT NULL
            THEN 1 ELSE 0 END) as empty_plain_text_count,
        SUM(CASE 
            WHEN LENGTH(TRIM(COALESCE(html, ''))) = 0 
             AND LENGTH(TRIM(COALESCE(plain_text, ''))) > 0 
             AND html IS NOT NULL
            THEN 1 ELSE 0 END) as empty_html_count,
        SUM(CASE 
            WHEN LENGTH(TRIM(COALESCE(plain_text, ''))) < 20 
            THEN 1 ELSE 0 END) as short_content_count
    FROM emails
)
SELECT 
    total_emails,
    no_content_count,
    ROUND(CAST(no_content_count AS FLOAT) / total_emails * 100, 2) as no_content_percentage,
    missing_plain_text_count,
    ROUND(CAST(missing_plain_text_count AS FLOAT) / total_emails * 100, 2) as missing_plain_text_percentage,
    missing_html_count,
    ROUND(CAST(missing_html_count AS FLOAT) / total_emails * 100, 2) as missing_html_percentage,
    empty_content_count,
    ROUND(CAST(empty_content_count AS FLOAT) / total_emails * 100, 2) as empty_content_percentage,
    empty_plain_text_count,
    ROUND(CAST(empty_plain_text_count AS FLOAT) / total_emails * 100, 2) as empty_plain_text_percentage,
    empty_html_count,
    ROUND(CAST(empty_html_count AS FLOAT) / total_emails * 100, 2) as empty_html_percentage,
    short_content_count,
    ROUND(CAST(short_content_count AS FLOAT) / total_emails * 100, 2) as short_content_percentage,
    total_emails - (
        no_content_count + 
        missing_plain_text_count + 
        missing_html_count + 
        empty_content_count + 
        empty_plain_text_count + 
        empty_html_count
    ) as complete_emails,
    ROUND(CAST(
        (total_emails - (
            no_content_count + 
            missing_plain_text_count + 
            missing_html_count + 
            empty_content_count + 
            empty_plain_text_count + 
            empty_html_count
        )) AS FLOAT) / total_emails * 100, 2) as complete_percentage
FROM email_stats;

-- Show current data quality summary
SELECT * FROM v_email_data_quality_summary;

-- Show sample of emails with issues
SELECT 
    email_id,
    email_date_readable,
    content_status,
    plain_text_length,
    html_length,
    SUBSTR(snippet_preview, 1, 100) as snippet_preview
FROM v_email_data_quality
ORDER BY email_date DESC
LIMIT 5;

-- Show emails with very short plain_text
SELECT 
    email_id,
    email_date_readable,
    plain_text_length,
    html_length,
    plain_text_content,
    SUBSTR(snippet_preview, 1, 100) as snippet_preview
FROM v_short_emails
LIMIT 5;

-- View to identify emails missing key trading signal words
CREATE VIEW IF NOT EXISTS missing_signal_words AS
WITH email_word_check AS (
    SELECT 
        e.id,
        e.plain_text,
        CASE 
            WHEN UPPER(plain_text) LIKE '%BUY%' OR UPPER(plain_text) LIKE '%SELL SHORT%' THEN 1 
            ELSE 0 
        END as has_action,
        CASE 
            WHEN UPPER(plain_text) LIKE '%STOP%' OR UPPER(plain_text) LIKE '%STOP-LOSS%' OR UPPER(plain_text) LIKE '%STOP LOSS%' THEN 1 
            ELSE 0 
        END as has_stop,
        CASE 
            WHEN UPPER(plain_text) LIKE '%TARGET%' OR UPPER(plain_text) LIKE '%PRICE TARGET%' THEN 1 
            ELSE 0 
        END as has_target,
        CASE 
            WHEN plain_text LIKE '%Having trouble viewing this email?%' 
              OR plain_text LIKE '%Forward email%'
              OR plain_text LIKE '%Click here%'
              OR plain_text LIKE '%constantcontact%'
              OR plain_text LIKE '%unsubscribe%' THEN 1
            ELSE 0
        END as is_system_message,
        CASE 
            WHEN UPPER(plain_text) LIKE '%SELL SHORT%' THEN 1
            ELSE 0
        END as is_short_sell
    FROM emails e
)
SELECT 
    id as email_id,
    plain_text,
    CASE
        WHEN is_system_message = 1 THEN 'System Message'
        WHEN is_short_sell = 1 THEN 'Short Sell Signal'
        WHEN has_action = 0 THEN 'Missing Buy/Sell Action'
        WHEN has_stop = 0 THEN 'Missing Stop Loss'
        WHEN has_target = 0 THEN 'Missing Target'
        ELSE 'Unknown Issue'
    END as issue_type,
    has_action,
    has_stop,
    has_target,
    is_system_message,
    is_short_sell
FROM email_word_check
WHERE has_action = 0 
   OR has_stop = 0 
   OR has_target = 0 
   OR is_system_message = 1
   OR is_short_sell = 1;

-- Show statistics about missing signal words
SELECT 
    issue_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM emails), 2) as percentage
FROM missing_signal_words
GROUP BY issue_type
ORDER BY count DESC;

-- Show examples of each issue type
SELECT 
    issue_type,
    email_id,
    SUBSTR(plain_text, 1, 200) as sample_text
FROM missing_signal_words
GROUP BY issue_type
LIMIT 10;

COMMIT; 