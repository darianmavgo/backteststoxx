package main

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/microcosm-cc/bluemonday"
)

// parseSignalsConcurrently processes emails to extract trading signals
func parseSignalsConcurrently(db *DB) error {
	log.Printf("Starting concurrent signal parsing")
	
	// Get emails that contain trading signal keywords
	emails, err := db.getSignalEmails()
	if err != nil {
		return fmt.Errorf("failed to get signal emails: %v", err)
	}

	log.Printf("Found %d emails with potential trading signals", len(emails))

	if len(emails) == 0 {
		log.Printf("No emails found with trading signal keywords")
		return nil
	}

	// Process emails concurrently
	numWorkers := 10 // Moderate concurrency for parsing
	jobs := make(chan EmailSignal, len(emails))
	results := make(chan error, len(emails))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			parseSignalWorker(workerID, jobs, results, db)
		}(i)
	}

	// Send jobs
	go func() {
		for _, email := range emails {
			jobs <- email
		}
		close(jobs)
	}()

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var errors []error
	var processedCount int
	for err := range results {
		if err != nil {
			errors = append(errors, err)
		} else {
			processedCount++
		}

		// Log progress every 25 emails
		if (processedCount+len(errors))%25 == 0 {
			log.Printf("Parsing progress: %d/%d emails processed", processedCount+len(errors), len(emails))
		}
	}

	log.Printf("Signal parsing complete: %d emails processed successfully, %d errors", processedCount, len(errors))

	if len(errors) > 0 {
		log.Printf("First few parsing errors: %v", errors[:min(5, len(errors))])
	}

	return nil
}

// parseSignalWorker processes individual emails for signal extraction
func parseSignalWorker(workerID int, jobs <-chan EmailSignal, results chan<- error, db *DB) {
	for email := range jobs {
		err := parseSignalFromEmail(workerID, email, db)
		results <- err
	}
}

// parseSignalFromEmail extracts trading signal from a single email
func parseSignalFromEmail(workerID int, email EmailSignal, db *DB) error {
	signal, cleanedText, err := extractTradingSignalWithText(email)
	if err != nil {
		return fmt.Errorf("failed to extract signal: %v", err)
	}

	// Always save to staging table, even if no valid signal found
	if signal == nil {
		// Create empty signal for failed parsing
		signal = &TradingSignal{
			EmailID:    email.ID,
			SignalDate: email.Date.Unix() * 1000,
			EntryDate:  email.Date.Add(24*time.Hour).Unix() * 1000,
		}
		log.Printf("Worker %d: No valid signal found in email %s, saving empty record", workerID, email.ID)
	} else {
		log.Printf("Worker %d: Parsed signal for %s - Ticker: %s, Buy: %.2f, Stop: %.2f, Target: %.2f",
			workerID, email.ID, signal.Ticker, signal.BuyPrice, signal.StopPrice, signal.TargetPrice)
	}

	// Save to parse_buy_stop_target staging table with cleaned text
	if err := saveToParseBuyStopTarget(email, signal, cleanedText, db); err != nil {
		return fmt.Errorf("failed to save parsed signal: %v", err)
	}

	return nil
}

// extractTradingSignalWithText parses HTML content and returns both signal and cleaned text
func extractTradingSignalWithText(email EmailSignal) (*TradingSignal, string, error) {
	htmlContent := email.HTML
	log.Printf("PARSING: Email ID %s, original HTML length: %d", email.ID, len(htmlContent))
	log.Printf("PARSING: Original HTML first 200 chars: %s", strings.ReplaceAll(htmlContent[:min(200, len(htmlContent))], "\n", " "))

	// Limit to first 1000 characters of HTML
	if len(htmlContent) > 1000 {
		htmlContent = htmlContent[:1000]
		log.Printf("PARSING: Truncated HTML to 1000 chars")
	}

	// Use bluemonday to properly strip all HTML/XML tags and entities
	p := bluemonday.StripTagsPolicy()
	plainText := p.Sanitize(htmlContent)
	log.Printf("PARSING: After bluemonday stripping, length: %d", len(plainText))
	log.Printf("PARSING: Stripped text first 200 chars: %s", strings.ReplaceAll(plainText[:min(200, len(plainText))], "\n", " "))

	// Clean up whitespace and normalize
	plainText = regexp.MustCompile(`[\r\n\t]+`).ReplaceAllString(plainText, " ")
	plainText = regexp.MustCompile(`\s+`).ReplaceAllString(plainText, " ")
	plainText = strings.TrimSpace(plainText)
	log.Printf("PARSING: After whitespace cleanup, length: %d", len(plainText))
	log.Printf("PARSING: Final cleaned text: %s", plainText[:min(200, len(plainText))])

	// Create cleaned lowercase version for raw_html field storage
	cleanedText := strings.ToLower(plainText)
	log.Printf("PARSING: Lowercase version for storage: %s", cleanedText[:min(100, len(cleanedText))])

	// Keep original case for ticker extraction, lowercase for price patterns
	htmlLower := strings.ToLower(plainText)

	// Initialize signal
	signal := &TradingSignal{
		EmailID:    email.ID,
		SignalDate: email.Date.Unix() * 1000,                   // Convert to milliseconds
		EntryDate:  email.Date.Add(24*time.Hour).Unix() * 1000, // Next day in milliseconds
	}

	// Extract ticker symbol using proven patterns from existing codebase
	extractTicker(signal, plainText, htmlLower)

	// Extract prices
	extractBuyPrice(signal, htmlLower)
	extractStopPrice(signal, htmlLower)
	extractTargetPrice(signal, htmlLower)

	// Validate signal - must have ticker and at least buy price
	log.Printf("PARSING: Final signal validation - Ticker: '%s', BuyPrice: %.2f, StopPrice: %.2f, TargetPrice: %.2f",
		signal.Ticker, signal.BuyPrice, signal.StopPrice, signal.TargetPrice)

	if signal.Ticker == "" || signal.BuyPrice == 0 {
		log.Printf("PARSING: Signal validation FAILED - missing ticker or buy price")
		return nil, cleanedText, nil // No valid signal found
	}

	log.Printf("PARSING: Signal validation PASSED - returning valid signal")
	return signal, cleanedText, nil
}

// extractTicker extracts ticker symbol using proven patterns
func extractTicker(signal *TradingSignal, plainText, htmlLower string) {
	// Common exclusion words that are not tickers
	exclusionWords := map[string]bool{
		"BUY": true, "SELL": true, "STOP": true, "TARGET": true, "PRICE": true,
		"ENTRY": true, "EXIT": true, "LOSS": true, "PROFIT": true, "TAKE": true,
		"AT": true, "TO": true, "FROM": true, "AND": true, "OR": true, "THE": true,
	}
	log.Printf("PARSING: Starting ticker extraction from text: %s", plainText[:min(100, len(plainText))])

	// Primary: Exchange format patterns (most reliable from SQL implementation)
	exchangePatterns := []string{
		`\(\s*NASDAQ:\s*([A-Z]{2,5})\s*\)`, // (NASDAQ: TICKER)
		`\(\s*NYSE:\s*([A-Z]{2,5})\s*\)`,   // (NYSE: TICKER)
		`NASDAQ:\s*([A-Z]{2,5})\b`,         // NASDAQ: TICKER
		`NYSE:\s*([A-Z]{2,5})\b`,           // NYSE: TICKER
	}

	for _, pattern := range exchangePatterns {
		re := regexp.MustCompile(pattern)
		if matches := re.FindStringSubmatch(plainText); len(matches) > 1 {
			ticker := strings.ToUpper(matches[1])
			log.Printf("PARSING: Found exchange pattern match: %s -> %s", pattern, ticker)
			if !exclusionWords[ticker] && len(ticker) >= 2 && len(ticker) <= 5 {
				signal.Ticker = ticker
				log.Printf("PARSING: Set ticker from exchange pattern: %s", ticker)
				return
			} else {
				log.Printf("PARSING: Rejected ticker %s (excluded or invalid length)", ticker)
			}
		}
	}

	// Secondary: Proximity patterns (from main.go implementation)
	if signal.Ticker == "" {
		log.Printf("PARSING: No ticker found in exchange patterns, trying proximity patterns")
		proximityPatterns := []string{
			`\b([A-Z]{2,5})\s*(?:buy|BUY)`,                  // Ticker followed by buy
			`(?:buy|BUY)\s*([A-Z]{2,5})\b`,                  // Buy followed by ticker
			`(?:symbol|ticker|stock)[:=]?\s*([A-Z]{2,5})\b`, // Explicit ticker mention
			`\b([A-Z]{2,5})\s+at\s+\$?\d+`,                  // Ticker at price
			`\b([A-Z]{2,5})\s*[-:]\s*\$?\d+`,                // Ticker: price or Ticker - price
		}

		for _, pattern := range proximityPatterns {
			re := regexp.MustCompile(pattern)
			if matches := re.FindStringSubmatch(plainText); len(matches) > 1 {
				ticker := strings.ToUpper(matches[1])
				log.Printf("PARSING: Found proximity pattern match: %s -> %s", pattern, ticker)
				if !exclusionWords[ticker] && len(ticker) >= 2 && len(ticker) <= 5 {
					signal.Ticker = ticker
					log.Printf("PARSING: Set ticker from proximity pattern: %s", ticker)
					return
				} else {
					log.Printf("PARSING: Rejected proximity ticker %s (excluded or invalid length)", ticker)
				}
			}
			// Also try with lowercase version for case variations
			if matches := re.FindStringSubmatch(htmlLower); len(matches) > 1 {
				ticker := strings.ToUpper(matches[1])
				log.Printf("PARSING: Found lowercase proximity pattern match: %s -> %s", pattern, ticker)
				if !exclusionWords[ticker] && len(ticker) >= 2 && len(ticker) <= 5 {
					signal.Ticker = ticker
					log.Printf("PARSING: Set ticker from lowercase proximity pattern: %s", ticker)
					return
				} else {
					log.Printf("PARSING: Rejected lowercase proximity ticker %s (excluded or invalid length)", ticker)
				}
			}
		}
	}
}

// extractBuyPrice extracts buy price from text
func extractBuyPrice(signal *TradingSignal, htmlLower string) {
	log.Printf("PARSING: Starting BUY price extraction from: %s", htmlLower[:min(100, len(htmlLower))])
	buyPatterns := []string{
		`buy.*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`entry.*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`buy\s+(?:at\s+)?\$?(\d+\.?\d*)`,
	}

	for _, pattern := range buyPatterns {
		re := regexp.MustCompile(pattern)
		if matches := re.FindStringSubmatch(htmlLower); len(matches) > 1 {
			log.Printf("PARSING: Found BUY price pattern match: %s -> %s", pattern, matches[1])
			if price, err := strconv.ParseFloat(matches[1], 64); err == nil {
				signal.BuyPrice = price
				log.Printf("PARSING: Set BUY price: %.2f", price)
				return
			} else {
				log.Printf("PARSING: Failed to parse BUY price %s: %v", matches[1], err)
			}
		}
	}
}

// extractStopPrice extracts stop loss price from text
func extractStopPrice(signal *TradingSignal, htmlLower string) {
	log.Printf("PARSING: Starting STOP price extraction")
	stopPatterns := []string{
		`(?:stop|stop[-\s]?loss).*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`(?:sl|s\.l\.).*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`stop\s+(?:at\s+)?\$?(\d+\.?\d*)`,
	}

	for _, pattern := range stopPatterns {
		re := regexp.MustCompile(pattern)
		if matches := re.FindStringSubmatch(htmlLower); len(matches) > 1 {
			log.Printf("PARSING: Found STOP price pattern match: %s -> %s", pattern, matches[1])
			if price, err := strconv.ParseFloat(matches[1], 64); err == nil {
				signal.StopPrice = price
				log.Printf("PARSING: Set STOP price: %.2f", price)
				return
			} else {
				log.Printf("PARSING: Failed to parse STOP price %s: %v", matches[1], err)
			}
		}
	}
}

// extractTargetPrice extracts target price from text
func extractTargetPrice(signal *TradingSignal, htmlLower string) {
	log.Printf("PARSING: Starting TARGET price extraction")
	targetPatterns := []string{
		`(?:target|take[-\s]?profit).*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`(?:tp|t\.p\.).*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`target\s+(?:at\s+)?\$?(\d+\.?\d*)`,
	}

	for _, pattern := range targetPatterns {
		re := regexp.MustCompile(pattern)
		if matches := re.FindStringSubmatch(htmlLower); len(matches) > 1 {
			log.Printf("PARSING: Found TARGET price pattern match: %s -> %s", pattern, matches[1])
			if price, err := strconv.ParseFloat(matches[1], 64); err == nil {
				signal.TargetPrice = price
				log.Printf("PARSING: Set TARGET price: %.2f", price)
				return
			} else {
				log.Printf("PARSING: Failed to parse TARGET price %s: %v", matches[1], err)
			}
		}
	}
}

// processSignalsConcurrently processes clean signals to trade_signals table
func processSignalsConcurrently(db *DB) error {
	log.Printf("Starting concurrent signal processing")
	
	// Get clean signals from parse_buy_stop_target
	signals, err := db.getCleanSignals()
	if err != nil {
		return fmt.Errorf("failed to get clean signals: %v", err)
	}

	log.Printf("Found %d clean signals to process", len(signals))

	if len(signals) == 0 {
		log.Printf("No clean signals found for processing")
		return nil
	}

	// Process signals concurrently
	numWorkers := 5 // Lower concurrency for database operations
	jobs := make(chan CleanSignal, len(signals))
	results := make(chan error, len(signals))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			processSignalWorker(workerID, jobs, results, db)
		}(i)
	}

	// Send jobs
	go func() {
		for _, signal := range signals {
			jobs <- signal
		}
		close(jobs)
	}()

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var errors []error
	var processedCount int
	for err := range results {
		if err != nil {
			errors = append(errors, err)
		} else {
			processedCount++
		}

		// Log progress every 20 signals
		if (processedCount+len(errors))%20 == 0 {
			log.Printf("Processing progress: %d/%d signals processed", processedCount+len(errors), len(signals))
		}
	}

	log.Printf("Signal processing complete: %d signals processed successfully, %d errors", processedCount, len(errors))

	if len(errors) > 0 {
		log.Printf("First few processing errors: %v", errors[:min(5, len(errors))])
	}

	return nil
}

// processSignalWorker processes individual clean signals
func processSignalWorker(workerID int, jobs <-chan CleanSignal, results chan<- error, db *DB) {
	for signal := range jobs {
		err := upsertToTradeSignals(signal, db, workerID)
		results <- err
	}
}