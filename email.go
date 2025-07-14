package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"sync"

	"google.golang.org/api/gmail/v1"
)

// processEmail extracts content from a Gmail message
func processEmail(message *gmail.Message) (*gmail.Message, error) {
	// Extract message content
	if err := extractMessageContent(message); err != nil {
		return nil, fmt.Errorf("failed to extract message content: %v", err)
	}
	return message, nil
}

// extractMessageContent extracts the content from Gmail message payload
func extractMessageContent(message *gmail.Message) error {
	if message.Payload == nil {
		return fmt.Errorf("message payload is nil")
	}

	// Try to extract content from different parts of the message
	content := extractContent(message.Payload)
	
	// Store the extracted content in snippet for now
	// This is a simplified approach - in production you'd want proper content storage
	if content != "" {
		message.Snippet = content
	}

	return nil
}

// extractContent recursively extracts content from message parts
func extractContent(part *gmail.MessagePart) string {
	var content strings.Builder

	// Check if this part has text content
	if part.Body != nil && part.Body.Data != "" {
		if strings.Contains(part.MimeType, "text/") {
			decoded, err := base64.URLEncoding.DecodeString(part.Body.Data)
			if err == nil {
				content.WriteString(string(decoded))
			}
		}
	}

	// Recursively process parts
	for _, subPart := range part.Parts {
		subContent := extractContent(subPart)
		if subContent != "" {
			content.WriteString(subContent)
		}
	}

	return content.String()
}

// downloadAllEmailsConcurrently fetches emails from Gmail API with concurrency
func downloadAllEmailsConcurrently(db *DB) error {
	log.Printf("Starting concurrent email download from %s", targetSender)
	
	ctx := context.Background()
	service, err := getGmailService(ctx)
	if err != nil {
		return fmt.Errorf("failed to get Gmail service: %v", err)
	}

	// Build query to get emails from target sender
	query := fmt.Sprintf("from:%s", targetSender)
	log.Printf("Gmail query: %s", query)

	// Get list of message IDs
	var messageIDs []string
	pageToken := ""
	
	for {
		call := service.Users.Messages.List("me").Q(query).MaxResults(500)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		
		response, err := call.Do()
		if err != nil {
			return fmt.Errorf("failed to list messages: %v", err)
		}

		for _, message := range response.Messages {
			messageIDs = append(messageIDs, message.Id)
		}

		if response.NextPageToken == "" {
			break
		}
		pageToken = response.NextPageToken
		
		log.Printf("Fetched batch of %d message IDs, total so far: %d", len(response.Messages), len(messageIDs))
	}

	log.Printf("Found %d total messages from %s", len(messageIDs), targetSender)

	if len(messageIDs) == 0 {
		log.Printf("No messages found from %s", targetSender)
		return nil
	}

	// Process messages concurrently
	numWorkers := 50 // High concurrency for Gmail API
	jobs := make(chan string, len(messageIDs))
	results := make(chan error, len(messageIDs))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			downloadEmailWorker(workerID, service, jobs, results, db)
		}(i)
	}

	// Send jobs
	go func() {
		for _, messageID := range messageIDs {
			jobs <- messageID
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
	var successCount int
	for err := range results {
		if err != nil {
			errors = append(errors, err)
		} else {
			successCount++
		}

		// Log progress every 100 messages
		if (successCount+len(errors))%100 == 0 {
			log.Printf("Progress: %d/%d messages processed", successCount+len(errors), len(messageIDs))
		}
	}

	log.Printf("Email download complete: %d messages processed successfully, %d errors", 
		successCount, len(errors))

	if len(errors) > 0 {
		log.Printf("First few errors: %v", errors[:min(5, len(errors))])
	}

	return nil
}

// downloadEmailWorker processes individual email messages
func downloadEmailWorker(workerID int, service *gmail.Service, jobs <-chan string, results chan<- error, db *DB) {
	for messageID := range jobs {
		err := downloadSingleEmail(workerID, service, messageID, db)
		results <- err
	}
}

// downloadSingleEmail fetches and saves a single email
func downloadSingleEmail(workerID int, service *gmail.Service, messageID string, db *DB) error {
	// Get the full message
	message, err := service.Users.Messages.Get("me", messageID).Format("full").Do()
	if err != nil {
		return fmt.Errorf("worker %d: failed to get message %s: %v", workerID, messageID, err)
	}

	// Save to email_landing table first (simplified staging)
	if err := db.saveEmailToLanding(message); err != nil {
		return fmt.Errorf("worker %d: failed to save message to landing: %v", workerID, err)
	}

	return nil
}

// enrichEmailsConcurrently fetches full email data and saves to emails table
func enrichEmailsConcurrently(db *DB) error {
	log.Printf("Starting concurrent email enrichment")
	
	// Get thread IDs from email_landing
	threadIDs, err := db.getThreadIDsFromLanding()
	if err != nil {
		return fmt.Errorf("failed to get thread IDs: %v", err)
	}

	log.Printf("Found %d thread IDs to enrich", len(threadIDs))

	if len(threadIDs) == 0 {
		log.Printf("No thread IDs found for enrichment")
		return nil
	}

	ctx := context.Background()
	service, err := getGmailService(ctx)
	if err != nil {
		return fmt.Errorf("failed to get Gmail service: %v", err)
	}

	// Process thread IDs concurrently
	numWorkers := 25 // Moderate concurrency for full email fetching
	jobs := make(chan string, len(threadIDs))
	results := make(chan error, len(threadIDs))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			enrichEmailWorker(workerID, service, jobs, results, db)
		}(i)
	}

	// Send jobs
	go func() {
		for _, threadID := range threadIDs {
			jobs <- threadID
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

		// Log progress every 10 threads
		if (processedCount+len(errors))%10 == 0 {
			log.Printf("Progress: %d/%d threads processed", processedCount+len(errors), len(threadIDs))
		}
	}

	log.Printf("Enrichment complete: %d threads processed successfully, %d errors", processedCount, len(errors))

	if len(errors) > 0 {
		log.Printf("First few errors: %v", errors[:min(5, len(errors))])
	}

	return nil
}

// enrichEmailWorker processes individual thread IDs for enrichment
func enrichEmailWorker(workerID int, service *gmail.Service, jobs <-chan string, results chan<- error, db *DB) {
	for threadID := range jobs {
		err := enrichSingleThread(workerID, service, threadID, db)
		results <- err
	}
}

// enrichSingleThread fetches full email data for a thread and saves to emails table
func enrichSingleThread(workerID int, service *gmail.Service, threadID string, db *DB) error {
	// Get messages in the thread
	thread, err := service.Users.Threads.Get("me", threadID).Do()
	if err != nil {
		return fmt.Errorf("worker %d: failed to get thread %s: %v", workerID, threadID, err)
	}

	// Process each message in the thread
	for _, message := range thread.Messages {
		// Get full message content
		fullMessage, err := service.Users.Messages.Get("me", message.Id).Format("full").Do()
		if err != nil {
			log.Printf("Worker %d: failed to get full message %s: %v", workerID, message.Id, err)
			continue
		}

		// Save to emails table with all fields
		if err := db.upsertFullEmailToDB(fullMessage); err != nil {
			log.Printf("Worker %d: failed to save full email %s: %v", workerID, message.Id, err)
			continue
		}
	}

	return nil
}