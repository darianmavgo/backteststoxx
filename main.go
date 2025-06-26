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
	credentialsFile = "/Users/darianhickman/Documents/Github/backteststoxx/client_secret_2_914016029840-24qpupahd54i01jt8kfvalmj2114kbh9.apps.googleusercontent.com.json"
	// credentialsFile = "client_secret_914016029840-24qpupahd54i01jt8kfvalmj2114kbh9.apps.googleusercontent.com.json"
	tokenDir    = ".credentials"
	tokenFile   = ".credentials/token.json"
	dbFile      = "backteststoxx_emails.db"
	targetLabel = "backteststoxx"
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

	// Create the emails table with the new schema
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS emails (
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
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	// Create indexes
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_thread_id ON emails(thread_id);
		CREATE INDEX IF NOT EXISTS idx_date ON emails(date);
		CREATE INDEX IF NOT EXISTS idx_subject ON emails(subject);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to create indexes: %v", err)
	}

	return db, nil
}

// saveEmailToDB saves an email to the SQLite database
func (db *DB) saveEmailToDB(msg *gmail.Message) error {
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

	// Save to database
	stmt, err := db.Prepare(`
		INSERT INTO emails (id, thread_id, subject, from_address, to_address, date, plain_text, html, label_ids)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(
		msg.Id,
		msg.ThreadId,
		subject,
		from,
		to,
		msg.InternalDate,
		plainText,
		html,
		strings.Join(msg.LabelIds, ","),
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
	http.HandleFunc("/batchget", func(w http.ResponseWriter, r *http.Request) {
		batchGetHandler(w, r, db)
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
					margin: 10px 0;
				}
				.button:hover {
					background-color: #357abd;
				}
			</style>
		</head>
		<body>
			<h1>Gmail Backtest Email Processor</h1>
			<p>Process your backtest emails from Gmail.</p>
			<div>
				<a href="/login" class="button">Login with Google</a>
			</div>
			<div id="actions" style="display:none;">
				<a href="/batchget" class="button">Fetch Emails</a>
				<a href="/fixdate" class="button">Fix Dates</a>
			</div>
		</body>
	</html>
	`
	fmt.Fprint(w, html)
}

func handleGoogleLogin(w http.ResponseWriter, r *http.Request) {
	url := config.AuthCodeURL(oauthStateString)
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
	token, err := config.Exchange(context.Background(), code)
	if err != nil {
		fmt.Printf("Code exchange failed with '%s'\n", err)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
		return
	}

	// Save the token
	if err := saveToken(tokenFile, token); err != nil {
		log.Printf("Unable to save token: %v", err)
		http.Error(w, "Failed to save authentication token", http.StatusInternalServerError)
		return
	}

	// Redirect to home page with success message
	http.Redirect(w, r, "/?success=true", http.StatusTemporaryRedirect)
}

// Update getGmailClient to use the stored token
func getGmailClient(ctx context.Context) (*http.Client, error) {
	token, err := tokenFromFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %v", err)
	}

	return config.Client(ctx, token), nil
}

func batchGetHandler(w http.ResponseWriter, r *http.Request, db *DB) {
	// Get Gmail service
	ctx := context.Background()
	client, err := getGmailClient(ctx)
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to get Gmail client: %v", err),
		})
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

	// Get label ID
	labelID, err := getLabelID(srv, targetLabel)
	if err != nil {
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to get label ID: %v", err),
		})
		return
	}

	// Fetch emails
	if err := fetchEmailsWithLabel(srv, labelID, db); err != nil {
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
		sendJSONResponse(w, HandlerResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to get Gmail client: %v", err),
		})
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

// fetchEmailsWithLabel retrieves all emails with the specified label
func fetchEmailsWithLabel(srv *gmail.Service, labelID string, db *DB) error {
	messages, err := srv.Users.Messages.List("me").LabelIds(labelID).Do()
	if err != nil {
		return fmt.Errorf("unable to retrieve messages: %v", err)
	}

	if len(messages.Messages) == 0 {
		return fmt.Errorf("no messages found with label")
	}

	// Create channels for concurrent processing
	numWorkers := 10
	jobs := make(chan *gmail.Message, len(messages.Messages))
	results := make(chan result, len(messages.Messages))

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
	for _, msg := range messages.Messages {
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

	log.Printf("Successfully processed %d messages", processedCount)
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

func getLabelID(srv *gmail.Service, labelName string) (string, error) {
	labels, err := srv.Users.Labels.List("me").Do()
	if err != nil {
		return "", fmt.Errorf("unable to retrieve labels: %v", err)
	}

	for _, label := range labels.Labels {
		if label.Name == labelName {
			return label.Id, nil
		}
	}

	return "", fmt.Errorf("label '%s' not found", labelName)
}

func createTables(db *sql.DB) error {
	// Create the emails table with the new schema
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS emails (
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
	`)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	// Create indexes
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_thread_id ON emails(thread_id);
		CREATE INDEX IF NOT EXISTS idx_date ON emails(date);
		CREATE INDEX IF NOT EXISTS idx_subject ON emails(subject);
	`)
	if err != nil {
		return fmt.Errorf("failed to create indexes: %v", err)
	}

	return nil
}

func processEmail(message *gmail.Message) (*gmail.Message, error) {
	// Extract message content
	if err := extractMessageContent(message); err != nil {
		return nil, fmt.Errorf("failed to extract content: %v", err)
	}
	return message, nil
}
