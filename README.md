# Backtest STOXX Email Processor

A Go application that fetches and stores emails with the "backteststoxx" label from Gmail into a SQLite database. The application provides a simple web interface for authentication and email processing.

## Technology Stack

- **Backend**: Go (Golang)
- **Database**: SQLite3 with WAL (Write-Ahead Logging) mode
- **Authentication**: OAuth2 with Gmail API
- **Web Interface**: Simple HTML/CSS

## Features

- OAuth2 authentication with Gmail
- Fetches emails with specific label ("backteststoxx")
- Stores email metadata and content in SQLite database
- Robust date parsing from email headers and content
- Concurrent email processing using goroutines
- Web interface for easy interaction

## Prerequisites

- Go 1.x
- SQLite3
- Gmail account with OAuth2 credentials
- "backteststoxx" label set up in Gmail

## Project Structure

```
backteststoxx/
├── main.go                 # Main application code
├── backteststoxx_emails.db # SQLite database
├── client_secret_*.json    # OAuth2 credentials
└── token.json             # Stored OAuth2 token
```

## Database Schema

```sql
CREATE TABLE emails (
    id TEXT PRIMARY KEY,
    thread_id TEXT,
    subject TEXT,
    from_address TEXT,
    to_address TEXT,
    date INTEGER,
    plain_text TEXT,
    html TEXT,
    label_ids TEXT,
    UNIQUE(id)
)
```

## API Endpoints

- `/` - Home page with authentication and action buttons
- `/login` - Initiates OAuth2 authentication flow
- `/callback` - OAuth2 callback handler
- `/batchget` - Fetches and processes emails with the target label
- `/fixdate` - Updates email dates using Gmail's internal date

## Workflow

1. **Authentication**:
   - User clicks "Login with Google" button
   - OAuth2 flow redirects to Gmail for authorization
   - Application stores OAuth token for future use

2. **Email Fetching**:
   - User clicks "Fetch Emails" button
   - Application queries Gmail API for emails with "backteststoxx" label
   - Emails are processed concurrently using goroutines
   - Email metadata and content are stored in SQLite

3. **Date Fixing**:
   - User clicks "Fix Dates" button
   - Application updates email dates using Gmail's internal date
   - Handles various date formats and fallback mechanisms

## Performance Features

- WAL mode for better concurrent write performance
- Connection pooling for database operations
- Concurrent email processing with goroutines
- Efficient date parsing with multiple format support
- Indexed database columns for faster queries

## Security Features

- OAuth2 state verification
- Secure token storage
- Prepared SQL statements to prevent injection
- HTTPS support (when configured)

## Error Handling

- Robust error handling for API calls
- Graceful fallbacks for date parsing
- Transaction support for database operations
- Detailed error logging

## Getting Started

1. Clone the repository
2. Set up OAuth2 credentials in Google Cloud Console
3. Place the credentials JSON file in the project root
4. Run the application:
   ```bash
   go run main.go
   ```
5. Access the web interface at `http://localhost:8080`

## Environment Setup

1. Create OAuth2 credentials:
   - Go to Google Cloud Console
   - Enable Gmail API
   - Create OAuth2 credentials
   - Download and rename to `client_secret_*.json`

2. Set up Gmail:
   - Create "backteststoxx" label
   - Apply label to relevant emails

## Contributing

Feel free to submit issues and enhancement requests.

## License

[Add your license here] 