package main

import (
	"fmt"
	"log"
	"net/http"
)

// executeSQLParsing runs the proven SQL parsing logic
func executeSQLParsing(db *DB) error {
	log.Printf("Starting SQL-based parsing using proven extraction logic")

	// Step 1: Extract tickers using exchange format patterns
	if err := extractTickersSQL(db); err != nil {
		return fmt.Errorf("ticker extraction failed: %v", err)
	}

	// Step 2: Extract prices using position-based parsing
	if err := extractPricesSQL(db); err != nil {
		return fmt.Errorf("price extraction failed: %v", err)
	}

	// Step 3: Show results
	if err := showExtractionResults(db); err != nil {
		return fmt.Errorf("failed to show results: %v", err)
	}

	log.Printf("SQL-based parsing completed successfully")
	return nil
}

// extractTickersSQL executes the proven ticker extraction logic
func extractTickersSQL(db *DB) error {
	log.Printf("Extracting tickers using proven SQL logic...")

	// First clear existing tickers
	if _, err := db.Exec("UPDATE trade_signals SET ticker = NULL"); err != nil {
		return fmt.Errorf("failed to clear tickers: %v", err)
	}

	// Execute the proven ticker extraction query
	tickerExtractionSQL := `
		WITH email_content AS (
			-- Get plain_text content for searching
			SELECT 
				e.id as email_id,
				COALESCE(e.html, '') as email_text
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
					WHEN UPPER(email_text) LIKE '%NASDAQ:%' AND UPPER(email_text) LIKE '%(%'
					THEN TRIM(SUBSTR(
						SUBSTR(UPPER(email_text), INSTR(UPPER(email_text), 'NASDAQ:') + 7),
						1,
						INSTR(SUBSTR(UPPER(email_text), INSTR(UPPER(email_text), 'NASDAQ:') + 7), ')') - 1
					))
					-- NYSE format - strict uppercase match after colon  
					WHEN UPPER(email_text) LIKE '%NYSE:%' AND UPPER(email_text) LIKE '%(%'
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
				-- Must not be common words or abbreviations
				AND ticker NOT IN (
					'A', 'I', 'AT', 'BE', 'DO', 'GO', 'IF', 'IN', 'IS', 'IT', 'NO', 'OF', 'ON', 'OR', 
					'RE', 'SO', 'TO', 'UP', 'US', 'WE', 'PM', 'AM', 'EST', 'PST', 'GMT', 'UTC',
					'NEW', 'TOP', 'BUY', 'SELL', 'STOP', 'TAKE', 'PUT', 'CALL', 'THE', 'ALL',
					'ALERT', 'TRADE', 'STOCK', 'PRICE', 'HIGH', 'LOW', 'OPEN', 'CLOSE', 'FREE',
					'AND', 'FOR', 'FROM', 'INTO', 'NEXT', 'OUT', 'OVER', 'THIS', 'WITH', 'NEWS',
					'CEO', 'CFO', 'CTO', 'COO', 'IPO', 'ICO', 'ETF', 'ADR', 'NYSE', 'DJIA',
					'PICK', 'UPDATE', 'WEEKLY', 'TRIAL', 'SAVE'
				)
		)
		UPDATE trade_signals
		SET ticker = (
			SELECT ticker 
			FROM valid_tickers 
			WHERE valid_tickers.email_id = trade_signals.email_id
		)
		WHERE EXISTS (
			SELECT 1 
			FROM valid_tickers 
			WHERE valid_tickers.email_id = trade_signals.email_id
		)`

	if _, err := db.Exec(tickerExtractionSQL); err != nil {
		return fmt.Errorf("failed to execute ticker extraction: %v", err)
	}

	// Get ticker extraction stats
	var totalSignals, signalsWithTickers int
	err := db.QueryRow(`
		SELECT 
			COUNT(*) as total_signals,
			SUM(CASE WHEN ticker IS NOT NULL THEN 1 ELSE 0 END) as signals_with_tickers
		FROM trade_signals
	`).Scan(&totalSignals, &signalsWithTickers)

	if err != nil {
		return fmt.Errorf("failed to get ticker stats: %v", err)
	}

	percentage := float64(signalsWithTickers) / float64(totalSignals) * 100
	log.Printf("Ticker extraction: %d/%d signals (%.1f%%) now have tickers", 
		signalsWithTickers, totalSignals, percentage)

	return nil
}

// extractPricesSQL executes the proven price extraction logic
func extractPricesSQL(db *DB) error {
	log.Printf("Extracting prices using proven SQL logic...")

	// Execute the proven price extraction query
	priceExtractionSQL := `
		WITH valid_emails AS (
			-- Get emails with sufficient content and valid tickers
			SELECT 
				e.id as email_id,
				ts.ticker,
				UPPER(TRIM(COALESCE(e.html, ''))) as email_text
			FROM emails e
			JOIN trade_signals ts ON e.id = ts.email_id
			WHERE LENGTH(TRIM(COALESCE(e.html, ''))) > 20
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
				-- Extract first number after BUY (simplified version)
				CASE 
					WHEN buy_segment LIKE '%AT %' THEN
						CAST(TRIM(REPLACE(REPLACE(REPLACE(
							SUBSTR(buy_segment, INSTR(buy_segment, 'AT ') + 3, 20),
							'$', ''), ' ', ''), ',', '')) AS DECIMAL)
					WHEN buy_segment LIKE '%@ %' THEN
						CAST(TRIM(REPLACE(REPLACE(REPLACE(
							SUBSTR(buy_segment, INSTR(buy_segment, '@ ') + 2, 20),
							'$', ''), ' ', ''), ',', '')) AS DECIMAL)
					WHEN buy_segment LIKE '%$%' THEN
						CAST(TRIM(REPLACE(REPLACE(REPLACE(
							SUBSTR(buy_segment, INSTR(buy_segment, '$') + 1, 20),
							'$', ''), ' ', ''), ',', '')) AS DECIMAL)
				END as buy_price,
				-- Extract first number after STOP
				CASE 
					WHEN stop_segment LIKE '%AT %' THEN
						CAST(TRIM(REPLACE(REPLACE(REPLACE(
							SUBSTR(stop_segment, INSTR(stop_segment, 'AT ') + 3, 20),
							'$', ''), ' ', ''), ',', '')) AS DECIMAL)
					WHEN stop_segment LIKE '%@ %' THEN
						CAST(TRIM(REPLACE(REPLACE(REPLACE(
							SUBSTR(stop_segment, INSTR(stop_segment, '@ ') + 2, 20),
							'$', ''), ' ', ''), ',', '')) AS DECIMAL)
					WHEN stop_segment LIKE '%$%' THEN
						CAST(TRIM(REPLACE(REPLACE(REPLACE(
							SUBSTR(stop_segment, INSTR(stop_segment, '$') + 1, 20),
							'$', ''), ' ', ''), ',', '')) AS DECIMAL)
				END as stop_price,
				-- Extract first number after TARGET
				CASE 
					WHEN target_segment LIKE '%AT %' THEN
						CAST(TRIM(REPLACE(REPLACE(REPLACE(
							SUBSTR(target_segment, INSTR(target_segment, 'AT ') + 3, 20),
							'$', ''), ' ', ''), ',', '')) AS DECIMAL)
					WHEN target_segment LIKE '%@ %' THEN
						CAST(TRIM(REPLACE(REPLACE(REPLACE(
							SUBSTR(target_segment, INSTR(target_segment, '@ ') + 2, 20),
							'$', ''), ' ', ''), ',', '')) AS DECIMAL)
					WHEN target_segment LIKE '%$%' THEN
						CAST(TRIM(REPLACE(REPLACE(REPLACE(
							SUBSTR(target_segment, INSTR(target_segment, '$') + 1, 20),
							'$', ''), ' ', ''), ',', '')) AS DECIMAL)
				END as target_price
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
				-- Basic price relationship validation (with tolerance)
				AND target_price >= buy_price * 0.9  -- Allow 10% tolerance
				AND buy_price >= stop_price * 0.9    -- Allow 10% tolerance
		)
		UPDATE trade_signals
		SET 
			buy_price = (
				SELECT buy_price 
				FROM validated_prices 
				WHERE validated_prices.email_id = trade_signals.email_id
				AND validated_prices.ticker = trade_signals.ticker
			),
			stop_price = (
				SELECT stop_price
				FROM validated_prices 
				WHERE validated_prices.email_id = trade_signals.email_id
				AND validated_prices.ticker = trade_signals.ticker
			),
			target_price = (
				SELECT target_price
				FROM validated_prices 
				WHERE validated_prices.email_id = trade_signals.email_id
				AND validated_prices.ticker = trade_signals.ticker
			)
		WHERE EXISTS (
			SELECT 1 
			FROM validated_prices 
			WHERE validated_prices.email_id = trade_signals.email_id
			AND validated_prices.ticker = trade_signals.ticker
		)`

	if _, err := db.Exec(priceExtractionSQL); err != nil {
		return fmt.Errorf("failed to execute price extraction: %v", err)
	}

	// Get price extraction stats
	var totalWithTickers, withBuyPrice, withStopPrice, withTargetPrice, completeSignals int
	err := db.QueryRow(`
		SELECT 
			SUM(CASE WHEN ticker IS NOT NULL THEN 1 ELSE 0 END) as signals_with_tickers,
			SUM(CASE WHEN buy_price IS NOT NULL THEN 1 ELSE 0 END) as signals_with_buy_price,
			SUM(CASE WHEN stop_price IS NOT NULL THEN 1 ELSE 0 END) as signals_with_stop_price,
			SUM(CASE WHEN target_price IS NOT NULL THEN 1 ELSE 0 END) as signals_with_target_price,
			SUM(CASE WHEN buy_price IS NOT NULL AND stop_price IS NOT NULL AND target_price IS NOT NULL THEN 1 ELSE 0 END) as complete_signals
		FROM trade_signals
		WHERE ticker IS NOT NULL
	`).Scan(&totalWithTickers, &withBuyPrice, &withStopPrice, &withTargetPrice, &completeSignals)

	if err != nil {
		return fmt.Errorf("failed to get price stats: %v", err)
	}

	if totalWithTickers > 0 {
		buyPercentage := float64(withBuyPrice) / float64(totalWithTickers) * 100
		completePercentage := float64(completeSignals) / float64(totalWithTickers) * 100
		
		log.Printf("Price extraction stats:")
		log.Printf("  - Buy prices: %d/%d (%.1f%%)", withBuyPrice, totalWithTickers, buyPercentage)
		log.Printf("  - Stop prices: %d/%d (%.1f%%)", withStopPrice, totalWithTickers, float64(withStopPrice)/float64(totalWithTickers)*100)
		log.Printf("  - Target prices: %d/%d (%.1f%%)", withTargetPrice, totalWithTickers, float64(withTargetPrice)/float64(totalWithTickers)*100)
		log.Printf("  - Complete signals: %d/%d (%.1f%%)", completeSignals, totalWithTickers, completePercentage)
	}

	return nil
}

// showExtractionResults displays sample results
func showExtractionResults(db *DB) error {
	log.Printf("Sample extraction results:")

	// Show sample of successfully extracted signals
	rows, err := db.Query(`
		SELECT 
			ticker,
			buy_price,
			stop_price,
			target_price,
			SUBSTR(e.html, 1, 200) as sample_text
		FROM trade_signals ts
		JOIN emails e ON ts.email_id = e.id
		WHERE ts.ticker IS NOT NULL
		  AND ts.buy_price IS NOT NULL
		  AND ts.stop_price IS NOT NULL
		  AND ts.target_price IS NOT NULL
		ORDER BY ts.signal_date DESC
		LIMIT 5
	`)
	if err != nil {
		return fmt.Errorf("failed to query successful extractions: %v", err)
	}
	defer rows.Close()

	log.Printf("✅ Successfully extracted signals:")
	for rows.Next() {
		var ticker string
		var buyPrice, stopPrice, targetPrice float64
		var sampleText string
		
		if err := rows.Scan(&ticker, &buyPrice, &stopPrice, &targetPrice, &sampleText); err != nil {
			log.Printf("Failed to scan result: %v", err)
			continue
		}
		
		log.Printf("  %s: Buy=%.2f, Stop=%.2f, Target=%.2f", ticker, buyPrice, stopPrice, targetPrice)
	}

	return nil
}

// HTTP handler for SQL-based parsing
func sqlParseSignalsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	db, err := setupDatabase()
	if err != nil {
		http.Error(w, fmt.Sprintf("Database setup failed: %v", err), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	if err := executeSQLParsing(db); err != nil {
		http.Error(w, fmt.Sprintf("SQL parsing failed: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, "SQL-based signal parsing completed successfully using proven extraction logic")
}