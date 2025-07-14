package main

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/api/gmail/v1"
)

// decodeBase64URL decodes base64 URL-encoded data
func decodeBase64URL(data string) ([]byte, error) {
	return base64.URLEncoding.DecodeString(data)
}

// DB represents our database connection
type DB struct {
	*sql.DB
}

func NewDB(db *sql.DB) *DB {
	return &DB{DB: db}
}

// setupDatabase initializes the database with required tables
func setupDatabase() (*DB, error) {
	db, err := sql.Open("sqlite3", dbFile+"?_journal_mode=WAL&_timeout=30000")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	// Create tables
	if err := createTables(db); err != nil {
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return NewDB(db), nil
}

// createTables creates all required database tables
func createTables(db *sql.DB) error {
	tables := []string{
		`CREATE TABLE IF NOT EXISTS email_landing (
			threadid TEXT PRIMARY KEY,
			content TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS emails (
			id TEXT PRIMARY KEY,
			thread_id TEXT,
			subject TEXT,
			date DATETIME,
			snippet TEXT,
			html TEXT,
			from_address TEXT,
			to_address TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS parse_buy_stop_target (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email_id TEXT UNIQUE,
			ticker TEXT,
			signal_date INTEGER,
			entry_date INTEGER,
			buy_price REAL,
			stop_price REAL,
			target_price REAL,
			raw_html TEXT,
			parsed_text TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS trade_signals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email_id TEXT UNIQUE,
			ticker TEXT NOT NULL,
			signal_date INTEGER NOT NULL,
			entry_date INTEGER NOT NULL,
			buy_price REAL NOT NULL,
			stop_price REAL,
			target_price REAL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, table := range tables {
		if _, err := db.Exec(table); err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
	}

	return nil
}

// saveEmailToLanding saves email to the landing table
func (db *DB) saveEmailToLanding(message *gmail.Message) error {
	stmt, err := db.Prepare(`
		INSERT OR REPLACE INTO email_landing (threadid, content) 
		VALUES (?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare landing statement: %v", err)
	}
	defer stmt.Close()

	content := message.Snippet
	if content == "" {
		content = "No content"
	}

	_, err = stmt.Exec(message.ThreadId, content)
	if err != nil {
		return fmt.Errorf("failed to insert into landing: %v", err)
	}

	return nil
}

// getThreadIDsFromLanding retrieves all thread IDs from email_landing
func (db *DB) getThreadIDsFromLanding() ([]string, error) {
	query := `SELECT threadid FROM email_landing ORDER BY threadid`
	
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query thread IDs: %v", err)
	}
	defer rows.Close()

	var threadIDs []string
	for rows.Next() {
		var threadID string
		if err := rows.Scan(&threadID); err != nil {
			log.Printf("Failed to scan thread ID: %v", err)
			continue
		}
		threadIDs = append(threadIDs, threadID)
	}

	return threadIDs, nil
}

// upsertFullEmailToDB saves complete email data to the emails table
func (db *DB) upsertFullEmailToDB(msg *gmail.Message) error {
	// Extract headers
	var subject, from, to string
	for _, header := range msg.Payload.Headers {
		switch strings.ToLower(header.Name) {
		case "subject":
			subject = header.Value
		case "from":
			from = header.Value
		case "to":
			to = header.Value
		}
	}

	// Parse date - InternalDate is already an int64 in milliseconds
	dateInt := msg.InternalDate
	if dateInt == 0 {
		log.Printf("No date found for message %s, using current time", msg.Id)
		dateInt = time.Now().Unix() * 1000 // fallback to current time
	}
	date := time.Unix(dateInt/1000, 0)

	// Extract HTML content
	htmlContent := extractHTMLFromMessage(msg)

	stmt, err := db.Prepare(`
		INSERT INTO emails (id, thread_id, subject, date, snippet, html, from_address, to_address)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			thread_id = excluded.thread_id,
			subject = excluded.subject,
			date = excluded.date,
			snippet = excluded.snippet,
			html = excluded.html,
			from_address = excluded.from_address,
			to_address = excluded.to_address
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare email statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		msg.Id,
		msg.ThreadId,
		subject,
		date,
		msg.Snippet,
		htmlContent,
		from,
		to,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert email: %v", err)
	}

	return nil
}

// extractHTMLFromMessage extracts HTML content from Gmail message
func extractHTMLFromMessage(msg *gmail.Message) string {
	if msg.Payload == nil {
		return ""
	}

	return extractHTMLFromPart(msg.Payload)
}

// extractHTMLFromPart recursively extracts HTML from message parts
func extractHTMLFromPart(part *gmail.MessagePart) string {
	// Check if this part is HTML
	if part.MimeType == "text/html" && part.Body != nil && part.Body.Data != "" {
		decoded, err := decodeBase64URL(part.Body.Data)
		if err == nil {
			return string(decoded)
		}
	}

	// Check parts recursively
	for _, subPart := range part.Parts {
		htmlContent := extractHTMLFromPart(subPart)
		if htmlContent != "" {
			return htmlContent
		}
	}

	return ""
}

// getSignalEmails retrieves emails that contain trading signal keywords
func (db *DB) getSignalEmails() ([]EmailSignal, error) {
	query := `
		SELECT id, thread_id, subject, date, html 
		FROM emails 
		WHERE html IS NOT NULL 
		AND LOWER(html) LIKE '%buy%'
		AND LOWER(html) LIKE '%stop%'
		AND LOWER(html) LIKE '%target%'
		ORDER BY date DESC
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query signal emails: %v", err)
	}
	defer rows.Close()

	var emails []EmailSignal
	for rows.Next() {
		var email EmailSignal
		var dateStr string
		
		if err := rows.Scan(&email.ID, &email.ThreadID, &email.Subject, &dateStr, &email.HTML); err != nil {
			log.Printf("Failed to scan email: %v", err)
			continue
		}

		// Parse date
		if parsedDate, err := time.Parse("2006-01-02 15:04:05", dateStr); err == nil {
			email.Date = parsedDate
		} else {
			log.Printf("Failed to parse date %s: %v", dateStr, err)
			email.Date = time.Now()
		}

		emails = append(emails, email)
	}

	return emails, nil
}

// saveToParseBuyStopTarget saves parsed data to the staging table
func saveToParseBuyStopTarget(email EmailSignal, signal *TradingSignal, htmlStripped string, db *DB) error {
	log.Printf("SAVING: Email ID %s, cleaned text length: %d", email.ID, len(htmlStripped))
	log.Printf("SAVING: Cleaned text preview: %s", htmlStripped[:min(100, len(htmlStripped))])
	stmt, err := db.Prepare(`
		INSERT INTO parse_buy_stop_target (email_id, ticker, signal_date, entry_date, buy_price, stop_price, target_price, raw_html, parsed_text)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(email_id) DO UPDATE SET
			ticker = excluded.ticker,
			signal_date = excluded.signal_date,
			entry_date = excluded.entry_date,
			buy_price = excluded.buy_price,
			stop_price = excluded.stop_price,
			target_price = excluded.target_price,
			raw_html = excluded.raw_html,
			parsed_text = excluded.parsed_text
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare parse statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		email.ID,
		signal.Ticker,
		signal.SignalDate,
		signal.EntryDate,
		signal.BuyPrice,
		signal.StopPrice,
		signal.TargetPrice,
		htmlStripped,
		"", // parsed_text field for future use
	)
	if err != nil {
		return fmt.Errorf("failed to insert parsed signal: %v", err)
	}

	return nil
}

// getCleanSignals retrieves clean signals from parse_buy_stop_target
func (db *DB) getCleanSignals() ([]CleanSignal, error) {
	query := `
		SELECT email_id, ticker, signal_date, entry_date, buy_price, stop_price, target_price
		FROM parse_buy_stop_target 
		WHERE ticker IS NOT NULL 
		AND ticker != ''
		AND buy_price IS NOT NULL 
		AND buy_price > 0
		AND stop_price IS NOT NULL 
		AND stop_price > 0
		AND target_price IS NOT NULL 
		AND target_price > 0
		ORDER BY signal_date DESC
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query clean signals: %v", err)
	}
	defer rows.Close()

	var signals []CleanSignal
	for rows.Next() {
		var signal CleanSignal
		
		if err := rows.Scan(
			&signal.EmailID,
			&signal.Ticker,
			&signal.SignalDate,
			&signal.EntryDate,
			&signal.BuyPrice,
			&signal.StopPrice,
			&signal.TargetPrice,
		); err != nil {
			log.Printf("Failed to scan clean signal: %v", err)
			continue
		}

		signals = append(signals, signal)
	}

	return signals, nil
}

// upsertToTradeSignals saves clean signal to trade_signals with date uniqueness
func upsertToTradeSignals(signal CleanSignal, db *DB, workerID int) error {
	// Check for existing signal with same date (uniqueness constraint)
	var existingID string
	checkQuery := `SELECT email_id FROM trade_signals WHERE signal_date = ? LIMIT 1`
	err := db.QueryRow(checkQuery, signal.SignalDate).Scan(&existingID)

	if err == nil {
		// Signal with same date exists, skip
		log.Printf("Worker %d: Skipping signal %s - date %d already exists (email_id: %s)",
			workerID, signal.EmailID, signal.SignalDate, existingID)
		return nil
	} else if err != sql.ErrNoRows {
		return fmt.Errorf("failed to check existing signal: %v", err)
	}

	// Insert new signal
	stmt, err := db.Prepare(`
		INSERT INTO trade_signals (email_id, ticker, signal_date, entry_date, buy_price, stop_price, target_price)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare trade signal statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		signal.EmailID,
		signal.Ticker,
		signal.SignalDate,
		signal.EntryDate,
		signal.BuyPrice,
		signal.StopPrice,
		signal.TargetPrice,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert clean signal: %v", err)
	}

	log.Printf("Worker %d: Processed clean signal %s - Ticker: %s, Buy: %.2f, Stop: %.2f, Target: %.2f",
		workerID, signal.EmailID, signal.Ticker, signal.BuyPrice, signal.StopPrice, signal.TargetPrice)

	return nil
}