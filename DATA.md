# DATA.md

Database analysis summary for backteststoxx_emails.db

## Database Overview

**Total Size**: SQLite database with 3 main tables and 14 analytical views  
**Data Period**: November 15, 2013 to June 6, 2025 (11+ years)  
**Primary Purpose**: Trading signal extraction and backtesting from Gmail emails

## Core Tables

### 1. emails (294 records)
- **Period**: 2013-11-15 to 2025-06-06 
- **Data Quality**: 99.66% complete (293/294 emails with full content)
- **Content**: Raw email data with plain_text, html, subject, dates
- **Issues**: Only 1 email (0.34%) has short content
- **Source**: Gmail API extraction with "backteststoxx" label

### 2. trade_signals (294 records)
- **Signal Extraction Rate**: 86.1% (253/294 emails have valid tickers)
- **Price Data**: 86.4% (254/294 emails have buy prices)
- **Complete Signals**: Majority have ticker + buy_price + stop_price + target_price
- **Price Range**: $5.00 - $745.00 (avg: $137.55)

#### Top 10 Most Frequent Tickers:
1. NVO (8 signals)
2. NVDA (6 signals) 
3. MSTR, AAPL (5 signals each)
4. PLTR, NET, LABU, ERX, DT, DDOG (4 signals each)

#### Recent Signals (Latest 5):
- LNTH: $72.00 buy, $72 stop, $120.00 target (June 2025)
- LRN: $155.00 buy, $143 stop, $190.00 target (March 2025)
- NVDA: $115.00 buy, $105 stop, $150.00 target (February 2025)
- YANG: $42.30 buy, $37.70 stop, $60.00 target (February 2025)

### 3. trade_signals_v1_1 (175 records)
- **Purpose**: Cleaned/deduplicated dataset for backtesting
- **Data Quality**: 100% complete (all records have ticker + prices)
- **Filtering**: Removed duplicates and incomplete signals from main table
- **Usage**: Primary dataset for Python backtesting scripts

## Backtest Results

### Summary Statistics
- **Total Backtest Results**: 193 trades across 2 batches
- **Batch 1**: 4 results (Aug 2022 - Apr 2023)
- **Batch 2**: 189 results (Nov 2020 - Jun 2025)

### Performance Metrics

#### Trade Outcomes:
- **Stop Losses**: 90 trades (46.6%)
- **No Entry**: 64 trades (33.2%) 
- **No Trade**: 36 trades (18.7%)
- **Target Hit**: 3 trades (1.6%)

#### Portfolio Performance:
- **Batch 1**: -6.5% return, 0% win rate
- **Batch 2**: -4.5% return, 3.4% win rate
- **Average Trade Duration**: 9.3-26.9 days
- **Sharpe Ratio**: Negative (-0.59 to -0.85)
- **Max Drawdown**: -9.2% to -12.8%

## Data Quality Assessment

### Email Content Quality:
- **Complete Emails**: 293/294 (99.66%)
- **Missing Content**: 0 emails
- **Short Content**: 1 email (0.34%)
- **System Messages**: Filtered out during processing

### Signal Extraction Quality:
- **Successfully Parsed**: 86.1% of emails
- **Missing Data**: 13.9% due to format variations or non-signal emails
- **Data Completeness**: v1.1 table achieves 100% completeness through filtering

## Database Features

### Analytical Views (14 total):
- `v_portfolio_summary`: Batch-level performance metrics
- `v_trade_signals_with_risk`: Risk/reward ratio calculations
- `v_email_data_quality_summary`: Content quality assessment
- `v_ticker_extraction_results`: Signal parsing success rates
- `missing_signal_words`: Identifies parsing failures
- Various other performance and quality monitoring views

### Indexing:
- Primary keys on all main tables
- Indexes on email dates, thread_ids, subjects
- Unique constraints on email_id relationships

## Key Insights

1. **High Data Quality**: 99.66% of emails successfully processed
2. **Consistent Signal Format**: 86%+ extraction rate indicates reliable email format
3. **Performance Challenges**: Low win rates (0-3.4%) suggest strategy or execution issues
4. **Recent Activity**: Continuous data through June 2025 shows active monitoring
5. **Diverse Portfolio**: 253 unique ticker symbols across 11+ years
6. **Risk Management**: Stop losses triggered in 46.6% of trades (proper risk control)

## Data Limitations

- **Low Win Rate**: Only 3 profitable trades out of 193 backtested
- **Entry Challenges**: 33% of signals resulted in "No Entry"
- **Missing Signals**: 13.9% of emails couldn't be parsed for complete signals
- **Historical Bias**: Backtest results may not reflect current market conditions