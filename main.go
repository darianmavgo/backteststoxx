package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/net/html"
	"golang.org/x/oauth2"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

const (
	credentialsFile = "./client_secret_356592720849-vvch7h4cp73nqsqe2pjvhl3gdp8eprcs.apps.googleusercontent.com.json"
	// credentialsFile = "client_secret_914016029840-24qpupahd54i01jt8kfvalmj2114kbh9.apps.googleusercontent.com.json"
	tokenDir     = ".credentials"
	tokenFile    = ".credentials/token.json"
	dbFile       = "backteststoxx_emails.db"
	targetSender = "drstoxx@drstoxx.com"
)

// result represents the result of processing an email
type result struct {
	msg *gmail.Message
	err error
}

// CredentialInfo stores the raw credential file information
type CredentialInfo struct {
	Web struct {
		ClientID                string   `json:"client_id"`
		ProjectID               string   `json:"project_id"`
		AuthURI                 string   `json:"auth_uri"`
		TokenURI                string   `json:"token_uri"`
		AuthProviderX509CertURL string   `json:"auth_provider_x509_cert_url"`
		ClientSecret            string   `json:"client_secret"`
		RedirectURIs            []string `json:"redirect_uris"`
		JavascriptOrigins       []string `json:"javascript_origins,omitempty"`
	} `json:"web"`
}

// OAuthClientInfo stores detailed information about the OAuth client and token
type OAuthClientInfo struct {
	ClientID        string    `json:"client_id"`
	ClientSecret    string    `json:"client_secret"`
	RedirectURI     string    `json:"redirect_uri"`
	AuthURL         string    `json:"auth_url"`
	TokenURL        string    `json:"token_url"`
	Scopes          []string  `json:"scopes"`
	TokenType       string    `json:"token_type"`
	AccessToken     string    `json:"access_token"`
	RefreshToken    string    `json:"refresh_token"`
	Expiry          time.Time `json:"expiry"`
	UserEmail       string    `json:"user_email"`
	UserID          string    `json:"user_id"`
	VerifiedEmail   bool      `json:"verified_email"`
	Picture         string    `json:"picture"`
	Locale          string    `json:"locale"`
	LastRefreshTime time.Time `json:"last_refresh_time"`
	TokenSource     string    `json:"token_source"`
	ApplicationName string    `json:"application_name"`
	ProjectID       string    `json:"project_id"`
}

// DB represents our database connection
type DB struct {
	*sql.DB
}

func NewDB(db *sql.DB) *DB {
	return &DB{DB: db}
}

// printCredentialInfo reads and prints all available information from the credentials file
func printCredentialInfo(credBytes []byte) (*CredentialInfo, error) {
	var credInfo CredentialInfo
	if err := json.Unmarshal(credBytes, &credInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal credentials: %v", err)
	}

	fmt.Printf("\n=== Credential Information ===\n")
	fmt.Printf("Project ID: %s\n", credInfo.Web.ProjectID)
	fmt.Printf("Client ID: %s\n", credInfo.Web.ClientID)
	fmt.Printf("Auth URI: %s\n", credInfo.Web.AuthURI)
	fmt.Printf("Token URI: %s\n", credInfo.Web.TokenURI)
	fmt.Printf("Redirect URIs: %v\n", credInfo.Web.RedirectURIs)

	return &credInfo, nil
}

// ensureTokenDir creates the token directory if it doesn't exist
func ensureTokenDir() error {
	tokenDir := "token"
	if _, err := os.Stat(tokenDir); os.IsNotExist(err) {
		return os.MkdirAll(tokenDir, 0700)
	}
	return nil
}

// initDB initializes the SQLite database with WAL mode and creates the schema
func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "backteststoxx_emails.db?_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Create the simplified email_landing table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS email_landing (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			threadid TEXT NOT NULL,
			content TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(threadid)
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	// Create index on threadid for fast lookups
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_email_landing_threadid ON email_landing(threadid);
		CREATE INDEX IF NOT EXISTS idx_email_landing_created_at ON email_landing(created_at);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create indexes: %v", err)
	}

	return db, nil
}

// saveEmailToDB saves an email to the simplified email_landing table
func (db *DB) saveEmailToDB(msg *gmail.Message) error {
	// Extract content from email
	var content string
	var processPayload func(*gmail.MessagePart) error
	processPayload = func(part *gmail.MessagePart) error {
		if part.MimeType == "text/plain" {
			if part.Body != nil && part.Body.Data != "" {
				data, err := base64.URLEncoding.DecodeString(part.Body.Data)
				if err != nil {
					return fmt.Errorf("failed to decode plain text: %v", err)
				}
				content = string(data)
			}
		} else if part.MimeType == "text/html" && content == "" {
			// Use HTML as fallback if no plain text
			if part.Body != nil && part.Body.Data != "" {
				data, err := base64.URLEncoding.DecodeString(part.Body.Data)
				if err != nil {
					return fmt.Errorf("failed to decode HTML: %v", err)
				}
				content = string(data)
			}
		}

		if part.Parts != nil {
			for _, subPart := range part.Parts {
				if err := processPayload(subPart); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := processPayload(msg.Payload); err != nil {
		return fmt.Errorf("failed to process payload: %v", err)
	}

	// Save to simplified email_landing table
	stmt, err := db.Prepare(`
		INSERT OR IGNORE INTO email_landing (threadid, content)
		VALUES (?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		msg.ThreadId,
		content,
	)
	if err != nil {
		return fmt.Errorf("failed to insert email: %v", err)
	}

	return nil
}

type HandlerResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Error   string `json:"error,omitempty"`
}

// OAuth configuration
var (
	oauthStateString = "random-state-string" // In production, generate this randomly per session
	config           *oauth2.Config
)

func main() {
	// Initialize database
	sqlDB, err := sql.Open("sqlite3", "backteststoxx_emails.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer sqlDB.Close()

	db := NewDB(sqlDB)

	// Create tables if they don't exist
	if err := createTables(db.DB); err != nil {
		log.Fatalf("Failed to create tables: %v", err)
	}

	// Initialize OAuth config
	credBytes, err := os.ReadFile(credentialsFile)
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	var credInfo CredentialInfo
	if err := json.Unmarshal(credBytes, &credInfo); err != nil {
		log.Fatalf("Unable to parse client secret file: %v", err)
	}

	config = &oauth2.Config{
		ClientID:     credInfo.Web.ClientID,
		ClientSecret: credInfo.Web.ClientSecret,
		RedirectURL:  "http://localhost:8080/callback",
		Scopes: []string{
			gmail.GmailReadonlyScope,
		},
		Endpoint: oauth2.Endpoint{
			AuthURL:  credInfo.Web.AuthURI,
			TokenURL: credInfo.Web.TokenURI,
		},
	}

	// Set up HTTP handlers
	http.HandleFunc("/", handleHome)
	http.HandleFunc("/login", handleGoogleLogin)
	http.HandleFunc("/callback", handleGoogleCallback)
	http.HandleFunc("/oauth-info", handleOAuthInfo)
	http.HandleFunc("/batchget", func(w http.ResponseWriter, r *http.Request) {
		batchGetHandler(w, r, db)
	})
	http.HandleFunc("/enrich-emails", func(w http.ResponseWriter, r *http.Request) {
		enrichEmailsHandler(w, r, db)
	})
	http.HandleFunc("/parse-signals", func(w http.ResponseWriter, r *http.Request) {
		parseSignalsHandler(w, r, db)
	})
	http.HandleFunc("/process-signals", func(w http.ResponseWriter, r *http.Request) {
		processSignalsHandler(w, r, db)
	})
	http.HandleFunc("/fixdate", func(w http.ResponseWriter, r *http.Request) {
		fixDateHandler(w, r, db)
	})

	// Start the server
	log.Printf("Server starting on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	var html = `
	<html>
		<head>
			<title>Gmail Backtest Email Processor</title>
			<style>
				body { 
					font-family: Arial, sans-serif; 
					margin: 40px auto;
					max-width: 800px;
					padding: 20px;
				}
				.button {
					display: inline-block;
					padding: 10px 20px;
					background-color: #4285f4;
					color: white;
					text-decoration: none;
					border-radius: 4px;
					margin: 10px 5px 10px 0;
				}
				.button:hover {
					background-color: #357abd;
				}
				.button.secondary {
					background-color: #34a853;
				}
				.button.secondary:hover {
					background-color: #2d8a47;
				}
				.alert {
					padding: 10px;
					margin: 10px 0;
					border-radius: 4px;
					background-color: #fff3cd;
					border: 1px solid #ffeaa7;
					color: #856404;
				}
			</style>
		</head>
		<body>
			<h1>Gmail Backtest Email Processor</h1>
			<p>Process your backtest emails from Gmail.</p>
			
			<div class="alert">
				<strong>Having OAuth issues?</strong> Check the <a href="/oauth-info">OAuth Configuration Info</a> to ensure your Google Cloud Console is set up correctly.
			</div>
			
			<div>
				<a href="/login" class="button">Login with Google</a>
				<a href="/oauth-info" class="button secondary">OAuth Config Info</a>
			</div>
			<div id="actions">
				<a href="/batchget" class="button">Fetch Emails</a>
				<a href="/enrich-emails" class="button">Enrich Full Email Data</a>
				<a href="/parse-signals" class="button">Parse Trading Signals</a>
				<a href="/process-signals" class="button">Process Clean Signals</a>
				<a href="/fixdate" class="button">Fix Dates</a>
			</div>
		</body>
	</html>
	`
	fmt.Fprint(w, html)
}

func handleGoogleLogin(w http.ResponseWriter, r *http.Request) {
	// Request offline access to get refresh token
	url := config.AuthCodeURL(oauthStateString, oauth2.AccessTypeOffline, oauth2.ApprovalForce)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
}

func handleGoogleCallback(w http.ResponseWriter, r *http.Request) {
	state := r.FormValue("state")
	if state != oauthStateString {
		fmt.Printf("Invalid oauth state, expected '%s', got '%s'\n", oauthStateString, state)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	code := r.FormValue("code")
	// Use context with offline access to get refresh token
	ctx := context.WithValue(context.Background(), oauth2.AccessTypeOffline, true)
	token, err := config.Exchange(ctx, code)
	if err != nil {
		fmt.Printf("Code exchange failed with '%s'\n", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	// Ensure we have a refresh token
	if token.RefreshToken == "" {
		log.Printf("Warning: No refresh token received. You may need to revoke access and re-authenticate.")
	}

	// Save the token
	if err := saveToken(tokenFile, token); err != nil {
		log.Printf("Unable to save token: %v", err)
		http.Error(w, "Failed to save authentication token", http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully authenticated and saved token")
	// Redirect to home page with success message
	http.Redirect(w, r, "/?success=true", http.StatusTemporaryRedirect)
}

// getGmailClient retrieves a Gmail client with automatic token refresh
func getGmailClient(ctx context.Context) (*http.Client, error) {
	token, err := tokenFromFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %v", err)
	}

	// Create a token source that will automatically refresh the token
	tokenSource := config.TokenSource(ctx, token)
	
	// Get a fresh token (this will refresh if needed)
	freshToken, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to refresh token: %v", err)
	}
	
	// Save the refreshed token if it was updated
	if freshToken.AccessToken != token.AccessToken {
		log.Printf("Token was refreshed, saving new token")
		if err := saveToken(tokenFile, freshToken); err != nil {
			log.Printf("Warning: failed to save refreshed token: %v", err)
		}
	}

	return config.Client(ctx, freshToken), nil
}

func batchGetHandler(w http.ResponseWriter, r *http.Request, db *DB) {
	// Get Gmail service
	ctx := context.Background()
	client, err := getGmailClient(ctx)
	if err != nil {
		// Check if this is an OAuth error requiring re-authentication
		if strings.Contains(err.Error(), "invalid_grant") || strings.Contains(err.Error(), "token") {
			sendJSONResponse(w, HandlerResponse{
				Success: false,
				Error:   "Authentication token expired. Please go to /login to re-authenticate.",
			})
		} else {
			sendJSONResponse(w, HandlerResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to get Gmail client: %v", err),
			})
		}
		return
	}

	srv, err := gmail.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to create Gmail service: %v", err),
		})
		return
	}

	// Fetch emails from specific sender
	if err := fetchEmailsFromSender(srv, targetSender, db); err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to fetch emails: %v", err),
		})
		return
	}

	sendJSONResponse(w, HandlerResponse{
		Success: true,
		Message: "Successfully fetched and stored emails",
	})
}

func fixDateHandler(w http.ResponseWriter, r *http.Request, db *DB) {
	// Get Gmail service
	ctx := context.Background()
	client, err := getGmailClient(ctx)
	if err != nil {
		// Check if this is an OAuth error requiring re-authentication
		if strings.Contains(err.Error(), "invalid_grant") || strings.Contains(err.Error(), "token") {
			sendJSONResponse(w, HandlerResponse{
				Success: false,
				Error:   "Authentication token expired. Please go to /login to re-authenticate.",
			})
		} else {
			sendJSONResponse(w, HandlerResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to get Gmail client: %v", err),
			})
		}
		return
	}

	srv, err := gmail.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to create Gmail service: %v", err),
		})
		return
	}

	// Update dates
	if err := updateEmailDates(srv, db); err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to update email dates: %v", err),
		})
		return
	}

	sendJSONResponse(w, HandlerResponse{
		Success: true,
		Message: "Successfully updated email dates",
	})
}

func updateEmailDates(srv *gmail.Service, db *DB) error {
	// Get all message IDs from the database
	rows, err := db.Query("SELECT id FROM emails")
	if err != nil {
		return fmt.Errorf("failed to query emails: %v", err)
	}
	defer rows.Close()

	var messageIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("failed to scan message ID: %v", err)
		}
		messageIDs = append(messageIDs, id)
	}

	// Create a worker pool to process messages
	numWorkers := 10
	jobs := make(chan string, len(messageIDs))
	results := make(chan error, len(messageIDs))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for messageID := range jobs {
				msg, err := srv.Users.Messages.Get("me", messageID).Do()
				if err != nil {
					results <- fmt.Errorf("failed to get message %s: %v", messageID, err)
					continue
				}

				// Update the date in the database using internalDate
				_, err = db.Exec("UPDATE emails SET date = ? WHERE id = ?", msg.InternalDate, messageID)
				if err != nil {
					results <- fmt.Errorf("failed to update date for message %s: %v", messageID, err)
					continue
				}

				results <- nil
			}
		}()
	}

	// Send jobs to workers
	for _, messageID := range messageIDs {
		jobs <- messageID
	}
	close(jobs)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
	var errors []error
	var processedCount int
	for err := range results {
		if err != nil {
			errors = append(errors, err)
		} else {
			processedCount++
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("encountered %d errors while updating dates: %v", len(errors), errors[0])
	}

	log.Printf("Successfully updated dates for %d messages", processedCount)
	return nil
}

func sendJSONResponse(w http.ResponseWriter, response HandlerResponse) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// extractPortFromRedirectURI extracts the port number from a redirect URI
func extractPortFromRedirectURI(uri string) (int, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return 0, fmt.Errorf("failed to parse URI: %v", err)
	}

	if u.Port() == "" {
		return 0, fmt.Errorf("no port specified in URI")
	}

	port, err := strconv.Atoi(u.Port())
	if err != nil {
		return 0, fmt.Errorf("invalid port number: %v", err)
	}

	return port, nil
}

// getTokenFromWeb requests a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) (*oauth2.Token, error) {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser:\n%v\n", authURL)

	fmt.Print("Enter the authorization code: ")
	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		return nil, fmt.Errorf("unable to read authorization code: %v", err)
	}

	tok, err := config.Exchange(context.Background(), authCode)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve token from web: %v", err)
	}
	return tok, nil
}

// tokenFromFile retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// saveToken saves a token to a file path.
func saveToken(path string, token *oauth2.Token) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("unable to cache oauth token: %v", err)
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(token)
}

// sanitizeFilename makes a string safe to use as a filename
func sanitizeFilename(name string) string {
	// Replace invalid characters with underscores
	invalid := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
	name = invalid.ReplaceAllString(name, "_")

	// Limit length
	if len(name) > 200 {
		name = name[:200]
	}

	return name
}

// fetchEmailsFromSender retrieves all emails from the specified sender
func fetchEmailsFromSender(srv *gmail.Service, sender string, db *DB) error {
	// Use Gmail search query to filter by sender
	query := fmt.Sprintf("from:%s", sender)
	messages, err := srv.Users.Messages.List("me").Q(query).MaxResults(5000).Do()
	if err != nil {
		return fmt.Errorf("unable to retrieve messages: %v", err)
	}

	if len(messages.Messages) == 0 {
		return fmt.Errorf("no messages found from sender: %s", sender)
	}

	log.Printf("Found %d messages from %s", len(messages.Messages), sender)

	// Handle pagination to get all emails
	allMessages := messages.Messages
	nextPageToken := messages.NextPageToken

	for nextPageToken != "" {
		moreMessages, err := srv.Users.Messages.List("me").Q(query).MaxResults(5000).PageToken(nextPageToken).Do()
		if err != nil {
			return fmt.Errorf("unable to retrieve more messages: %v", err)
		}

		allMessages = append(allMessages, moreMessages.Messages...)
		nextPageToken = moreMessages.NextPageToken
		log.Printf("Retrieved %d more messages, total now: %d", len(moreMessages.Messages), len(allMessages))
	}

	log.Printf("Total messages to process: %d", len(allMessages))

	// Create channels for concurrent processing
	numWorkers := 15 // Increased worker count for better concurrency
	jobs := make(chan *gmail.Message, len(allMessages))
	results := make(chan result, len(allMessages))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				message, err := srv.Users.Messages.Get("me", msg.Id).Do()
				if err != nil {
					results <- result{err: fmt.Errorf("failed to get message %s: %v", msg.Id, err)}
					continue
				}

				// Save directly to database using our DB type
				if err := db.saveEmailToDB(message); err != nil {
					results <- result{err: fmt.Errorf("failed to save message %s: %v", msg.Id, err)}
					continue
				}

				results <- result{msg: message}
			}
		}()
	}

	// Send jobs to workers
	for _, msg := range allMessages {
		jobs <- msg
	}
	close(jobs)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
	var errors []error
	var processedCount int
	for result := range results {
		if result.err != nil {
			errors = append(errors, result.err)
		} else {
			processedCount++
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("encountered %d errors while processing messages: %v", len(errors), errors[0])
	}

	log.Printf("Successfully processed %d out of %d messages from %s", processedCount, len(allMessages), sender)
	if len(errors) > 0 {
		log.Printf("Encountered %d errors during processing", len(errors))
	}
	return nil
}

// parseDate attempts to parse a date string using multiple formats
func parseDate(dateStr string) (time.Time, error) {
	// Common email date formats
	formats := []string{
		"Mon, 2 Jan 2006 15:04:05 -0700",
		"Mon, 02 Jan 2006 15:04:05 -0700",
		"2 Jan 2006 15:04:05 -0700",
		"02 Jan 2006 15:04:05 -0700",
		"Mon, 2 Jan 2006 15:04:05 MST",
		"Mon, 02 Jan 2006 15:04:05 MST",
		time.RFC1123Z,
		time.RFC822Z,
		time.RFC3339,
		"Mon, 2 Jan 2006 15:04:05 GMT",
		"Mon, 02 Jan 2006 15:04:05 GMT",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("could not parse date: %s", dateStr)
}

// extractDateFromHTML attempts to find a date in the HTML content
func extractDateFromHTML(htmlContent string) (time.Time, error) {
	doc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return time.Time{}, err
	}

	// Common date patterns in HTML
	datePatterns := []string{
		`\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?(?:\s+[+-]\d{4})?`,
		`\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?`,
		`\d{1,2}/\d{1,2}/\d{4}`,
	}

	var dates []string
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.TextNode {
			text := strings.TrimSpace(n.Data)
			for _, pattern := range datePatterns {
				re := regexp.MustCompile(pattern)
				if match := re.FindString(text); match != "" {
					dates = append(dates, match)
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)

	// Try to parse each found date
	for _, dateStr := range dates {
		if t, err := parseDate(dateStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("no valid date found in HTML")
}

// Process headers and extract date in fetchEmailsWithLabel function
func processEmailDate(email *gmail.Message, headers []*gmail.MessagePartHeader, htmlContent string) {
	var dateStr string
	for _, header := range headers {
		if header.Name == "Date" {
			dateStr = header.Value
			break
		}
	}

	// Try to parse the date from header
	if dateStr != "" {
		if parsedDate, err := parseDate(dateStr); err == nil {
			email.InternalDate = parsedDate.Unix()
			return
		}
	}

	// If header date parsing failed, try HTML content
	if htmlContent != "" {
		if parsedDate, err := extractDateFromHTML(htmlContent); err == nil {
			email.InternalDate = parsedDate.Unix()
			return
		}
	}

	// If all else fails, use current time and log warning
	email.InternalDate = time.Now().Unix()
	log.Printf("Warning: Could not parse date for message %s, using current time", email.Id)
}

// Modify the email processing part in fetchEmailsWithLabel
func extractMessageContent(message *gmail.Message) error {
	// Process the date with both header and HTML content
	var htmlContent string
	if message.Payload != nil && message.Payload.Body != nil {
		htmlContent = message.Payload.Body.Data
	}
	processEmailDate(message, message.Payload.Headers, htmlContent)
	return nil
}

// getLabelID function removed - no longer needed since we're filtering by sender

func createTables(db *sql.DB) error {
	// Create the simplified email_landing table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS email_landing (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			threadid TEXT NOT NULL,
			content TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(threadid)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create email_landing table: %v", err)
	}

	// Create the parse_buy_stop_target staging table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS parse_buy_stop_target (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email_id TEXT NOT NULL,
			ticker TEXT,
			signal_date INTEGER,
			entry_date INTEGER,
			buy_price REAL,
			stop_price REAL,
			target_price REAL,
			raw_html TEXT,
			parsed_text TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(email_id)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create parse_buy_stop_target table: %v", err)
	}

	// Create indexes
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_email_landing_threadid ON email_landing(threadid);
		CREATE INDEX IF NOT EXISTS idx_email_landing_created_at ON email_landing(created_at);
		CREATE INDEX IF NOT EXISTS idx_parse_buy_stop_target_email_id ON parse_buy_stop_target(email_id);
		CREATE INDEX IF NOT EXISTS idx_parse_buy_stop_target_ticker ON parse_buy_stop_target(ticker);
		CREATE INDEX IF NOT EXISTS idx_parse_buy_stop_target_signal_date ON parse_buy_stop_target(signal_date);
	`)
	if err != nil {
		return fmt.Errorf("failed to create indexes: %v", err)
	}

	return nil
}

func handleOAuthInfo(w http.ResponseWriter, r *http.Request) {
	// Get current redirect URI from config
	redirectURI := "http://localhost:8080/callback"
	if config != nil {
		redirectURI = config.RedirectURL
	}

	var html = fmt.Sprintf(`
	<html>
		<head>
			<title>OAuth Configuration Info</title>
			<style>
				body { 
					font-family: Arial, sans-serif; 
					margin: 40px auto;
					max-width: 900px;
					padding: 20px;
				}
				.config-box {
					background-color: #f8f9fa;
					border: 1px solid #dee2e6;
					border-radius: 8px;
					padding: 20px;
					margin: 20px 0;
				}
				.code {
					background-color: #f1f3f4;
					padding: 10px;
					border-radius: 4px;
					font-family: monospace;
					margin: 10px 0;
					word-break: break-all;
				}
				.step {
					background-color: #e8f0fe;
					border-left: 4px solid #4285f4;
					padding: 15px;
					margin: 15px 0;
				}
				.error {
					background-color: #fce8e6;
					border: 1px solid #f5c6cb;
					border-radius: 4px;
					padding: 15px;
					margin: 15px 0;
					color: #721c24;
				}
				.button {
					display: inline-block;
					padding: 10px 20px;
					background-color: #4285f4;
					color: white;
					text-decoration: none;
					border-radius: 4px;
					margin: 10px 5px 10px 0;
				}
				.button:hover {
					background-color: #357abd;
				}
			</style>
		</head>
		<body>
			<h1>OAuth Configuration Info</h1>
			
			<div class="error">
				<h3>🚨 Error 400: redirect_uri_mismatch</h3>
				<p>This error occurs when the redirect URI in your Google Cloud Console doesn't match what the application is using.</p>
			</div>

			<div class="config-box">
				<h3>Current Application Settings</h3>
				<p><strong>Redirect URI being used:</strong></p>
				<div class="code">%s</div>
				<p><strong>Credentials file:</strong></p>
				<div class="code">%s</div>
			</div>

			<h2>How to Fix This</h2>
			
			<div class="step">
				<h4>Step 1: Go to Google Cloud Console</h4>
				<p>Visit: <a href="https://console.cloud.google.com/apis/credentials" target="_blank">https://console.cloud.google.com/apis/credentials</a></p>
			</div>

			<div class="step">
				<h4>Step 2: Select Your Project</h4>
				<p>Make sure you're in the correct Google Cloud project that contains your OAuth 2.0 credentials.</p>
			</div>

			<div class="step">
				<h4>Step 3: Find Your OAuth 2.0 Client</h4>
				<p>Look for the OAuth 2.0 Client ID that matches your credentials file. Click on it to edit.</p>
			</div>

			<div class="step">
				<h4>Step 4: Add the Redirect URI</h4>
				<p>In the "Authorized redirect URIs" section, add this exact URI:</p>
				<div class="code">%s</div>
				<p><strong>Important:</strong> Copy and paste this exactly, including the protocol (http://) and port (:8080)</p>
			</div>

			<div class="step">
				<h4>Step 5: Save Changes</h4>
				<p>Click "Save" in the Google Cloud Console. Changes may take a few minutes to propagate.</p>
			</div>

			<div class="step">
				<h4>Step 6: Try Again</h4>
				<p>Wait 2-3 minutes, then try the login process again.</p>
			</div>

			<h2>Alternative Solutions</h2>
			
			<div class="config-box">
				<h3>Option 1: Use Different Port</h3>
				<p>If you already have a different redirect URI configured in Google Cloud Console, you can modify the application to use that port instead.</p>
			</div>

			<div class="config-box">
				<h3>Option 2: Use ngrok for External Access</h3>
				<p>If you need external access, you can use ngrok:</p>
				<div class="code">
					1. Install ngrok: https://ngrok.com/<br>
					2. Run: ngrok http 8080<br>
					3. Add the ngrok URL to Google Cloud Console<br>
					4. Update the application's redirect URI
				</div>
			</div>

			<div>
				<a href="/" class="button">← Back to Home</a>
				<a href="/login" class="button">Try Login Again</a>
			</div>
		</body>
	</html>
	`, redirectURI, credentialsFile, redirectURI)
	
	fmt.Fprint(w, html)
}

// upsertFullEmailToDB saves complete email data to the emails table
func (db *DB) upsertFullEmailToDB(msg *gmail.Message) error {
	// Extract headers
	var subject, from, to string
	for _, header := range msg.Payload.Headers {
		switch header.Name {
		case "Subject":
			subject = header.Value
		case "From":
			from = header.Value
		case "To":
			to = header.Value
		}
	}

	// Extract content
	var plainText, html string
	var processPayload func(*gmail.MessagePart) error
	processPayload = func(part *gmail.MessagePart) error {
		if part.MimeType == "text/plain" {
			if part.Body != nil && part.Body.Data != "" {
				data, err := base64.URLEncoding.DecodeString(part.Body.Data)
				if err != nil {
					return fmt.Errorf("failed to decode plain text: %v", err)
				}
				plainText = string(data)
			}
		} else if part.MimeType == "text/html" {
			if part.Body != nil && part.Body.Data != "" {
				data, err := base64.URLEncoding.DecodeString(part.Body.Data)
				if err != nil {
					return fmt.Errorf("failed to decode HTML: %v", err)
				}
				html = string(data)
			}
		}

		if part.Parts != nil {
			for _, subPart := range part.Parts {
				if err := processPayload(subPart); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := processPayload(msg.Payload); err != nil {
		return fmt.Errorf("failed to process payload: %v", err)
	}

	// Create snippet from plain text or HTML
	snippet := ""
	if plainText != "" {
		if len(plainText) > 200 {
			snippet = plainText[:200] + "..."
		} else {
			snippet = plainText
		}
	} else if html != "" {
		// Simple HTML to text conversion for snippet
		htmlStripped := regexp.MustCompile(`<[^>]*>`).ReplaceAllString(html, "")
		if len(htmlStripped) > 200 {
			snippet = htmlStripped[:200] + "..."
		} else {
			snippet = htmlStripped
		}
	}

	// Upsert to emails table (insert or update if exists)
	stmt, err := db.Prepare(`
		INSERT INTO emails (id, thread_id, subject, from_addr, to_addr, date, snippet, labels, plain_text, html)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			thread_id = excluded.thread_id,
			subject = excluded.subject,
			from_addr = excluded.from_addr,
			to_addr = excluded.to_addr,
			date = excluded.date,
			snippet = excluded.snippet,
			labels = excluded.labels,
			plain_text = excluded.plain_text,
			html = excluded.html
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare upsert statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		msg.Id,
		msg.ThreadId,
		subject,
		from,
		to,
		msg.InternalDate,
		snippet,
		strings.Join(msg.LabelIds, ","),
		plainText,
		html,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert email: %v", err)
	}

	return nil
}

// enrichEmailsHandler processes threadids from email_landing and enriches with full Gmail data
func enrichEmailsHandler(w http.ResponseWriter, r *http.Request, db *DB) {
	// Get Gmail service
	ctx := context.Background()
	client, err := getGmailClient(ctx)
	if err != nil {
		// Check if this is an OAuth error requiring re-authentication
		if strings.Contains(err.Error(), "invalid_grant") || strings.Contains(err.Error(), "token") {
			sendJSONResponse(w, HandlerResponse{
				Success: false,
				Error:   "Authentication token expired. Please go to /login to re-authenticate.",
			})
		} else {
			sendJSONResponse(w, HandlerResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to get Gmail client: %v", err),
			})
		}
		return
	}

	srv, err := gmail.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to create Gmail service: %v", err),
		})
		return
	}

	// Get all threadids from email_landing
	threadIDs, err := getThreadIDsFromLanding(db)
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to get thread IDs: %v", err),
		})
		return
	}

	if len(threadIDs) == 0 {
		sendJSONResponse(w, HandlerResponse{
			Success: true,
			Message: "No threads found in email_landing table",
		})
		return
	}

	log.Printf("Starting enrichment of %d threads with maximum concurrency", len(threadIDs))

	// Process threads with high concurrency
	if err := enrichThreadsWithEmails(srv, threadIDs, db); err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to enrich emails: %v", err),
		})
		return
	}

	sendJSONResponse(w, HandlerResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully processed %d threads", len(threadIDs)),
	})
}

// getThreadIDsFromLanding retrieves all threadids from email_landing table
func getThreadIDsFromLanding(db *DB) ([]string, error) {
	rows, err := db.Query("SELECT threadid FROM email_landing ORDER BY created_at")
	if err != nil {
		return nil, fmt.Errorf("failed to query thread IDs: %v", err)
	}
	defer rows.Close()

	var threadIDs []string
	for rows.Next() {
		var threadID string
		if err := rows.Scan(&threadID); err != nil {
			return nil, fmt.Errorf("failed to scan thread ID: %v", err)
		}
		threadIDs = append(threadIDs, threadID)
	}

	return threadIDs, nil
}

// enrichThreadsWithEmails processes threads concurrently and enriches with full email data
func enrichThreadsWithEmails(srv *gmail.Service, threadIDs []string, db *DB) error {
	// High concurrency settings
	numWorkers := 25 // Increased for maximum concurrency
	jobs := make(chan string, len(threadIDs))
	results := make(chan error, len(threadIDs))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for threadID := range jobs {
				if err := processThreadEmails(srv, threadID, db, workerID); err != nil {
					results <- fmt.Errorf("worker %d failed to process thread %s: %v", workerID, threadID, err)
					continue
				}
				results <- nil
			}
		}(i)
	}

	// Send jobs to workers
	for _, threadID := range threadIDs {
		jobs <- threadID
	}
	close(jobs)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
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
		// Log first few errors for debugging
		for i, err := range errors {
			if i < 5 { // Log first 5 errors
				log.Printf("Error %d: %v", i+1, err)
			}
		}
		return fmt.Errorf("encountered %d errors while processing threads (processed %d successfully)", len(errors), processedCount)
	}

	return nil
}

// processThreadEmails gets all emails in a thread and upserts them to emails table
func processThreadEmails(srv *gmail.Service, threadID string, db *DB, workerID int) error {
	// Get thread with all messages
	thread, err := srv.Users.Threads.Get("me", threadID).Format("full").Do()
	if err != nil {
		return fmt.Errorf("failed to get thread: %v", err)
	}

	// Process each message in the thread
	for _, msg := range thread.Messages {
		if err := db.upsertFullEmailToDB(msg); err != nil {
			return fmt.Errorf("failed to upsert message %s: %v", msg.Id, err)
		}
	}

	log.Printf("Worker %d: Processed thread %s with %d messages", workerID, threadID, len(thread.Messages))
	return nil
}

// EmailSignal represents an email that needs signal parsing
type EmailSignal struct {
	ID       string
	ThreadID string
	Subject  string
	Date     time.Time
	HTML     string
}

// TradingSignal represents extracted trading data
type TradingSignal struct {
	EmailID     string
	Ticker      string
	SignalDate  int64
	EntryDate   int64
	BuyPrice    float64
	StopPrice   float64
	TargetPrice float64
}

// parseSignalsHandler processes emails containing BUY signals and extracts trading data
func parseSignalsHandler(w http.ResponseWriter, r *http.Request, db *DB) {
	// Get emails containing BUY signals
	emails, err := getBuySignalEmails(db)
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to get BUY signal emails: %v", err),
		})
		return
	}

	if len(emails) == 0 {
		sendJSONResponse(w, HandlerResponse{
			Success: true,
			Message: "No emails found containing BUY signals",
		})
		return
	}

	log.Printf("Starting signal parsing for %d emails with BUY signals", len(emails))

	// Process emails with maximum concurrency
	processedCount, errorCount, err := parseSignalsConcurrently(emails, db)
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to parse signals: %v", err),
		})
		return
	}

	sendJSONResponse(w, HandlerResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully parsed %d emails, %d errors encountered", processedCount, errorCount),
	})
}

// getBuySignalEmails retrieves emails containing buy, stop, and target keywords in HTML content
func getBuySignalEmails(db *DB) ([]EmailSignal, error) {
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
		if err := rows.Scan(&email.ID, &email.ThreadID, &email.Subject, &email.Date, &email.HTML); err != nil {
			return nil, fmt.Errorf("failed to scan email: %v", err)
		}
		emails = append(emails, email)
	}

	return emails, nil
}

// parseSignalsConcurrently processes emails with maximum concurrency
func parseSignalsConcurrently(emails []EmailSignal, db *DB) (int, int, error) {
	// Maximum concurrency settings
	numWorkers := 30 // High concurrency for parsing
	jobs := make(chan EmailSignal, len(emails))
	results := make(chan error, len(emails))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for email := range jobs {
				if err := parseAndSaveSignal(email, db, workerID); err != nil {
					results <- fmt.Errorf("worker %d failed to parse email %s: %v", workerID, email.ID, err)
					continue
				}
				results <- nil
			}
		}(i)
	}

	// Send jobs to workers
	for _, email := range emails {
		jobs <- email
	}
	close(jobs)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
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

	// Log first few errors for debugging
	if len(errors) > 0 {
		for i, err := range errors {
			if i < 3 { // Log first 3 errors
				log.Printf("Parse error %d: %v", i+1, err)
			}
		}
	}

	return processedCount, len(errors), nil
}

// parseAndSaveSignal extracts trading signal from email HTML and saves to parse_buy_stop_target
func parseAndSaveSignal(email EmailSignal, db *DB, workerID int) error {
	// Parse trading signal from HTML
	signal, htmlStripped, err := extractTradingSignalWithText(email)
	if err != nil {
		return fmt.Errorf("failed to extract signal: %v", err)
	}

	// Always save to staging table, even if no valid signal found
	if signal == nil {
		// Create empty signal for failed parsing
		signal = &TradingSignal{
			EmailID:    email.ID,
			SignalDate: email.Date.Unix() * 1000,
			EntryDate:  email.Date.Add(24 * time.Hour).Unix() * 1000,
		}
		log.Printf("Worker %d: No valid signal found in email %s, saving empty record", workerID, email.ID)
	} else {
		log.Printf("Worker %d: Parsed signal for %s - Ticker: %s, Buy: %.2f, Stop: %.2f, Target: %.2f", 
			workerID, email.ID, signal.Ticker, signal.BuyPrice, signal.StopPrice, signal.TargetPrice)
	}

	// Save to parse_buy_stop_target staging table with cleaned text
	if err := saveToParseBuyStopTarget(email, signal, htmlStripped, db); err != nil {
		return fmt.Errorf("failed to save parsed signal: %v", err)
	}
	
	return nil
}

// extractTradingSignalWithText parses HTML content and returns both signal and stripped text
func extractTradingSignalWithText(email EmailSignal) (*TradingSignal, string, error) {
	html := email.HTML
	
	// Remove HTML tags and convert to lowercase for easier parsing
	htmlStripped := regexp.MustCompile(`<[^>]*>`).ReplaceAllString(html, " ")
	htmlStripped = regexp.MustCompile(`\s+`).ReplaceAllString(htmlStripped, " ")
	htmlStripped = strings.TrimSpace(htmlStripped)
	htmlLower := strings.ToLower(htmlStripped)
	
	// Initialize signal
	signal := &TradingSignal{
		EmailID:    email.ID,
		SignalDate: email.Date.Unix() * 1000, // Convert to milliseconds
		EntryDate:  email.Date.Add(24 * time.Hour).Unix() * 1000, // Next day in milliseconds
	}

	// Extract ticker symbol using proven patterns from existing codebase
	// Common exclusion words that are not tickers
	exclusionWords := map[string]bool{
		"BUY": true, "SELL": true, "STOP": true, "TARGET": true, "PRICE": true,
		"ENTRY": true, "EXIT": true, "LOSS": true, "PROFIT": true, "TAKE": true,
		"AT": true, "TO": true, "FROM": true, "AND": true, "OR": true, "THE": true,
	}
	
	// Primary: Exchange format patterns (most reliable from SQL implementation)
	exchangePatterns := []string{
		`\(\s*NASDAQ:\s*([A-Z]{2,5})\s*\)`, // (NASDAQ: TICKER)
		`\(\s*NYSE:\s*([A-Z]{2,5})\s*\)`,   // (NYSE: TICKER)
		`NASDAQ:\s*([A-Z]{2,5})\b`,         // NASDAQ: TICKER
		`NYSE:\s*([A-Z]{2,5})\b`,           // NYSE: TICKER
	}
	
	for _, pattern := range exchangePatterns {
		re := regexp.MustCompile(pattern)
		if matches := re.FindStringSubmatch(htmlStripped); len(matches) > 1 {
			ticker := strings.ToUpper(matches[1])
			if !exclusionWords[ticker] && len(ticker) >= 2 && len(ticker) <= 5 {
				signal.Ticker = ticker
				break
			}
		}
	}
	
	// Secondary: Proximity patterns (from main.go implementation)
	if signal.Ticker == "" {
		proximityPatterns := []string{
			`\b([A-Z]{2,5})\s*(?:buy|BUY)`,     // Ticker followed by buy
			`(?:buy|BUY)\s*([A-Z]{2,5})\b`,     // Buy followed by ticker
			`(?:symbol|ticker|stock)[:=]?\s*([A-Z]{2,5})\b`, // Explicit ticker mention
			`\b([A-Z]{2,5})\s+at\s+\$?\d+`,    // Ticker at price
			`\b([A-Z]{2,5})\s*[-:]\s*\$?\d+`,  // Ticker: price or Ticker - price
		}
		
		for _, pattern := range proximityPatterns {
			re := regexp.MustCompile(pattern)
			if matches := re.FindStringSubmatch(htmlStripped); len(matches) > 1 {
				ticker := strings.ToUpper(matches[1])
				if !exclusionWords[ticker] && len(ticker) >= 2 && len(ticker) <= 5 {
					signal.Ticker = ticker
					break
				}
			}
			// Also try with lowercase version for case variations
			if matches := re.FindStringSubmatch(htmlLower); len(matches) > 1 {
				ticker := strings.ToUpper(matches[1])
				if !exclusionWords[ticker] && len(ticker) >= 2 && len(ticker) <= 5 {
					signal.Ticker = ticker
					break
				}
			}
		}
	}

	// Extract BUY price - use lowercase for pattern matching
	buyPatterns := []string{
		`buy.*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`entry.*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`buy\s+(?:at\s+)?\$?(\d+\.?\d*)`,
	}
	
	for _, pattern := range buyPatterns {
		re := regexp.MustCompile(pattern)
		if matches := re.FindStringSubmatch(htmlLower); len(matches) > 1 {
			if price, err := strconv.ParseFloat(matches[1], 64); err == nil {
				signal.BuyPrice = price
				break
			}
		}
	}

	// Extract STOP LOSS price - use lowercase for pattern matching
	stopPatterns := []string{
		`(?:stop|stop[-\s]?loss).*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`(?:sl|s\.l\.).*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`stop\s+(?:at\s+)?\$?(\d+\.?\d*)`,
	}
	
	for _, pattern := range stopPatterns {
		re := regexp.MustCompile(pattern)
		if matches := re.FindStringSubmatch(htmlLower); len(matches) > 1 {
			if price, err := strconv.ParseFloat(matches[1], 64); err == nil {
				signal.StopPrice = price
				break
			}
		}
	}

	// Extract TARGET price - use lowercase for pattern matching
	targetPatterns := []string{
		`(?:target|take[-\s]?profit).*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`(?:tp|t\.p\.).*?(?:at|@|price|:)?\s*\$?(\d+\.?\d*)`,
		`target\s+(?:at\s+)?\$?(\d+\.?\d*)`,
	}
	
	for _, pattern := range targetPatterns {
		re := regexp.MustCompile(pattern)
		if matches := re.FindStringSubmatch(htmlLower); len(matches) > 1 {
			if price, err := strconv.ParseFloat(matches[1], 64); err == nil {
				signal.TargetPrice = price
				break
			}
		}
	}

	// Validate signal - must have ticker and at least buy price
	if signal.Ticker == "" || signal.BuyPrice == 0 {
		return nil, htmlStripped, nil // No valid signal found
	}

	return signal, htmlStripped, nil
}

// saveToParseBuyStopTarget saves parsed data to the staging table
func saveToParseBuyStopTarget(email EmailSignal, signal *TradingSignal, htmlStripped string, db *DB) error {
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
		return fmt.Errorf("failed to prepare insert statement: %v", err)
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
		email.HTML,
		htmlStripped,
	)
	if err != nil {
		return fmt.Errorf("failed to insert parsed signal: %v", err)
	}

	return nil
}

// processSignalsHandler processes clean signals from parse_buy_stop_target to trade_signals
func processSignalsHandler(w http.ResponseWriter, r *http.Request, db *DB) {
	// Get clean signals from parse_buy_stop_target
	cleanSignals, err := getCleanSignalsFromParsing(db)
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to get clean signals: %v", err),
		})
		return
	}

	if len(cleanSignals) == 0 {
		sendJSONResponse(w, HandlerResponse{
			Success: true,
			Message: "No clean signals found in parse_buy_stop_target table",
		})
		return
	}

	log.Printf("Starting processing of %d clean signals with maximum concurrency", len(cleanSignals))

	// Process clean signals with high concurrency
	processedCount, errorCount, err := processCleanSignalsConcurrently(cleanSignals, db)
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to process signals: %v", err),
		})
		return
	}

	sendJSONResponse(w, HandlerResponse{
		Success: true,
		Message: fmt.Sprintf("Successfully processed %d clean signals, %d errors encountered", processedCount, errorCount),
	})
}

// CleanSignal represents a validated signal from parse_buy_stop_target
type CleanSignal struct {
	EmailID     string
	Ticker      string
	SignalDate  int64
	EntryDate   int64
	BuyPrice    float64
	StopPrice   float64
	TargetPrice float64
}

// getCleanSignalsFromParsing retrieves signals with non-null ticker, buy, stop, target
func getCleanSignalsFromParsing(db *DB) ([]CleanSignal, error) {
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
		if err := rows.Scan(&signal.EmailID, &signal.Ticker, &signal.SignalDate, &signal.EntryDate, &signal.BuyPrice, &signal.StopPrice, &signal.TargetPrice); err != nil {
			return nil, fmt.Errorf("failed to scan clean signal: %v", err)
		}
		signals = append(signals, signal)
	}

	return signals, nil
}

// processCleanSignalsConcurrently processes clean signals with maximum concurrency
func processCleanSignalsConcurrently(signals []CleanSignal, db *DB) (int, int, error) {
	// Maximum concurrency settings
	numWorkers := 20 // High concurrency for processing
	jobs := make(chan CleanSignal, len(signals))
	results := make(chan error, len(signals))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for signal := range jobs {
				if err := upsertToTradeSignals(signal, db, workerID); err != nil {
					results <- fmt.Errorf("worker %d failed to process signal %s: %v", workerID, signal.EmailID, err)
					continue
				}
				results <- nil
			}
		}(i)
	}

	// Send jobs to workers
	for _, signal := range signals {
		jobs <- signal
	}
	close(jobs)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results
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

	// Log first few errors for debugging
	if len(errors) > 0 {
		for i, err := range errors {
			if i < 3 { // Log first 3 errors
				log.Printf("Process error %d: %v", i+1, err)
			}
		}
	}

	return processedCount, len(errors), nil
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
		ON CONFLICT(email_id) DO UPDATE SET
			ticker = excluded.ticker,
			signal_date = excluded.signal_date,
			entry_date = excluded.entry_date,
			buy_price = excluded.buy_price,
			stop_price = excluded.stop_price,
			target_price = excluded.target_price
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare upsert statement: %v", err)
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

func processEmail(message *gmail.Message) (*gmail.Message, error) {
	// Extract message content
	if err := extractMessageContent(message); err != nil {
		return nil, fmt.Errorf("failed to extract content: %v", err)
	}
	return message, nil
}
