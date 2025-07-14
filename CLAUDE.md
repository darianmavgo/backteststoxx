# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a hybrid Go/Python trading email processor and backtesting system. The main application is a Go web server that fetches emails from Gmail with the "backteststoxx" label, parses trading signals, and stores them in SQLite. Python scripts handle backtesting and data analysis.

## Development Commands

### Go Application
```bash
# Run the main Go application (web server on :8080)
go run main.go

# Build the Go binary
go build -o backteststoxx main.go

# Install Go dependencies
go mod tidy
```

### Python Environment
```bash
# Set up Python virtual environment and dependencies
./setup_env.sh

# Activate virtual environment
source venv/bin/activate

# Run backtest analysis (requires active venv)
python backtest_trades.py
```

### Database Operations
```bash
# Apply SQL migrations (run from sql/ directory)
sqlite3 ../backteststoxx_emails.db < create_trade_signals_v1_1.sql
```

## Architecture

### Data Flow
1. **Email Ingestion**: Go web server (`main.go`) authenticates with Gmail OAuth2, fetches emails with "backteststoxx" label
2. **Signal Parsing**: Emails are parsed for trading signals (ticker, buy/stop/target prices, dates)
3. **Database Storage**: Parsed signals stored in SQLite (`backteststoxx_emails.db`) using structured tables
4. **Data Processing**: SQL scripts in `sql/` directory transform and clean data
5. **Backtesting**: Python scripts (`backtest_trades.py`) load signals and run backtests using yfinance data

### Key Components

#### Go Web Server (`main.go`)
- OAuth2 Gmail authentication with multiple credential files
- RESTful endpoints: `/login`, `/callback`, `/batchget`, `/fixdate`
- Concurrent email processing with goroutines
- SQLite database with WAL mode for performance

#### Database Schema
- `emails` table: Raw email storage (id, subject, content, dates)
- `trade_signals` table: Parsed trading signals
- `trade_signals_v1_1` table: Cleaned/deduplicated signals for backtesting

#### Python Analysis
- `backtest_trades.py`: Uses backtesting library with yfinance data
- `get_all_backteststoxx_emails_.py`: Legacy Google Colab extraction script

### Database Tables
- `emails`: Raw email metadata and content
- `trade_signals`: Extracted trading signals with prices and dates  
- `trade_signals_v1_1`: Cleaned dataset for backtesting (non-null tickers/prices, deduplicated)

## Configuration Files

### OAuth2 Credentials
- Multiple `client_secret_*.json` files for Gmail API access
- `token.json` stores OAuth2 refresh tokens
- Hardcoded credential file path in `main.go:27`

### Dependencies
- `go.mod`: Go module with Gmail API, SQLite driver, OAuth2 libraries
- `setup_env.sh`: Python environment setup with google-api-python-client

## Important Notes

- The Go application serves a web interface on `localhost:8080` for Gmail authentication and email processing
- SQLite database uses WAL mode for concurrent access
- Python scripts require the virtual environment and assume the SQLite database exists
- SQL migrations in `sql/` directory should be applied manually as needed
- Email parsing handles various date formats and trading signal patterns from email content