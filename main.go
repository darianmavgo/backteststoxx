package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/oauth2"
)

const (
	credentialsFile = "./client_secret_356592720849-vvch7h4cp73nqsqe2pjvhl3gdp8eprcs.apps.googleusercontent.com.json"
	tokenDir        = ".credentials"
	tokenFile       = ".credentials/token.json"
	dbFile          = "backteststoxx_emails.db"
	targetSender    = "drstoxx@drstoxx.com"
)

// Global configuration variable
var config *oauth2.Config

// Type definitions
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

type EmailSignal struct {
	ID       string
	ThreadID string
	Subject  string
	Date     time.Time
	HTML     string
}

type TradingSignal struct {
	EmailID     string
	Ticker      string
	SignalDate  int64
	EntryDate   int64
	BuyPrice    float64
	StopPrice   float64
	TargetPrice float64
}

type CleanSignal struct {
	EmailID     string
	Ticker      string
	SignalDate  int64
	EntryDate   int64
	BuyPrice    float64
	StopPrice   float64
	TargetPrice float64
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}


// HTTP Handlers
func homeHandler(w http.ResponseWriter, r *http.Request) {
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>Gmail Email Processor</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 800px; margin: 0 auto; }
        .button { display: inline-block; padding: 10px 20px; margin: 10px 5px; 
                  background-color: #007cba; color: white; text-decoration: none; 
                  border-radius: 5px; border: none; cursor: pointer; }
        .button:hover { background-color: #005a8b; }
        .button.secondary { background-color: #28a745; }
        .button.secondary:hover { background-color: #1e7e34; }
        .button.warning { background-color: #ffc107; color: black; }
        .button.warning:hover { background-color: #e0a800; }
        .info { background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }
        .endpoint { background-color: #e9ecef; padding: 10px; margin: 5px 0; border-radius: 3px; font-family: monospace; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📧 Gmail Email Processor</h1>
        
        <div class="info">
            <h3>🔧 Setup & Authentication</h3>
            <p>First, you need to authenticate with Gmail API:</p>
            <a href="/login" class="button">🔐 Login with Gmail</a>
        </div>

        <div class="info">
            <h3>📥 Email Processing Pipeline</h3>
            <p>Process emails through the complete pipeline:</p>
            
            <div class="endpoint">
                <strong>1. Download Emails:</strong> POST /download-emails<br>
                <small>Downloads all emails from drstoxx@drstoxx.com to email_landing table</small>
            </div>
            
            <div class="endpoint">
                <strong>2. Enrich Emails:</strong> POST /enrich-emails<br>
                <small>Fetches full email content and saves to emails table</small>
            </div>
            
            <div class="endpoint">
                <strong>2b. Enrich Emails v1.2:</strong> POST /enrich-emails-v1-2<br>
                <small>⭐ Re-downloads emails from emails_v1_1 thread_ids with InternalDate field</small>
            </div>
            
            <div class="endpoint">
                <strong>3. Parse Signals (Go):</strong> POST /parse-signals<br>
                <small>Extracts trading signals from email HTML using Go parsing logic</small>
            </div>
            
            <div class="endpoint">
                <strong>3. Parse Signals (SQL):</strong> POST /sql-parse-signals<br>
                <small>⭐ Extracts trading signals using proven SQL parsing logic</small>
            </div>
            
            <div class="endpoint">
                <strong>4. Process Signals:</strong> POST /process-signals<br>
                <small>Processes clean signals to trade_signals table with uniqueness</small>
            </div>
        </div>

        <div class="info">
            <h3>🚀 Quick Actions</h3>
            <button onclick="downloadEmails()" class="button">📥 Download Emails</button>
            <button onclick="enrichEmails()" class="button secondary">📧 Enrich Emails</button>
            <button onclick="enrichEmailsV1_2()" class="button secondary">⭐ Enrich Emails v1.2</button>
            <button onclick="parseSignals()" class="button warning">🔍 Parse Signals (Go)</button>
            <button onclick="sqlParseSignals()" class="button warning">⭐ Parse Signals (SQL)</button>
            <button onclick="processSignals()" class="button secondary">⚡ Process Signals</button>
        </div>

        <div class="info">
            <h3>📊 Status</h3>
            <div id="status">Ready to process emails...</div>
        </div>
    </div>

    <script>
        function updateStatus(message) {
            document.getElementById('status').innerHTML = message;
        }

        function downloadEmails() {
            updateStatus('📥 Downloading emails...');
            fetch('/download-emails', { method: 'POST' })
                .then(response => response.text())
                .then(data => updateStatus('✅ ' + data))
                .catch(error => updateStatus('❌ Error: ' + error));
        }

        function enrichEmails() {
            updateStatus('📧 Enriching emails...');
            fetch('/enrich-emails', { method: 'POST' })
                .then(response => response.text())
                .then(data => updateStatus('✅ ' + data))
                .catch(error => updateStatus('❌ Error: ' + error));
        }

        function enrichEmailsV1_2() {
            updateStatus('⭐ Enriching emails v1.2 with InternalDate...');
            fetch('/enrich-emails-v1-2', { method: 'POST' })
                .then(response => response.text())
                .then(data => updateStatus('✅ ' + data))
                .catch(error => updateStatus('❌ Error: ' + error));
        }

        function parseSignals() {
            updateStatus('🔍 Parsing signals...');
            fetch('/parse-signals', { method: 'POST' })
                .then(response => response.text())
                .then(data => updateStatus('✅ ' + data))
                .catch(error => updateStatus('❌ Error: ' + error));
        }

        function sqlParseSignals() {
            updateStatus('⭐ Parsing signals with SQL...');
            fetch('/sql-parse-signals', { method: 'POST' })
                .then(response => response.text())
                .then(data => updateStatus('✅ ' + data))
                .catch(error => updateStatus('❌ Error: ' + error));
        }

        function processSignals() {
            updateStatus('⚡ Processing signals...');
            fetch('/process-signals', { method: 'POST' })
                .then(response => response.text())
                .then(data => updateStatus('✅ ' + data))
                .catch(error => updateStatus('❌ Error: ' + error));
        }
    </script>
</body>
</html>`
	fmt.Fprint(w, html)
}

func downloadEmailsHandler(w http.ResponseWriter, r *http.Request) {
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

	if err := downloadAllEmailsConcurrently(db); err != nil {
		http.Error(w, fmt.Sprintf("Email download failed: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, "Email download completed successfully")
}

func enrichEmailsHandler(w http.ResponseWriter, r *http.Request) {
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

	if err := enrichEmailsConcurrently(db); err != nil {
		http.Error(w, fmt.Sprintf("Email enrichment failed: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, "Email enrichment completed successfully")
}

func parseSignalsHandler(w http.ResponseWriter, r *http.Request) {
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

	if err := parseSignalsConcurrently(db); err != nil {
		http.Error(w, fmt.Sprintf("Signal parsing failed: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, "Signal parsing completed successfully")
}

func processSignalsHandler(w http.ResponseWriter, r *http.Request) {
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

	if err := processSignalsConcurrently(db); err != nil {
		http.Error(w, fmt.Sprintf("Signal processing failed: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, "Signal processing completed successfully")
}

func enrichEmailsV1_2Handler(w http.ResponseWriter, r *http.Request) {
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

	if err := enrichEmailsV1_2Concurrently(db); err != nil {
		http.Error(w, fmt.Sprintf("emails_v1_2 enrichment failed: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, "emails_v1_2 enrichment completed successfully")
}

func main() {
	// Create credentials directory if it doesn't exist
	if err := os.MkdirAll(tokenDir, 0700); err != nil {
		log.Fatalf("Unable to create credentials directory: %v", err)
	}

	// Load OAuth configuration
	var err error
	config, err = loadCredentials(credentialsFile)
	if err != nil {
		log.Fatalf("Failed to load credentials: %v", err)
	}

	log.Printf("OAuth configuration loaded successfully")
	log.Printf("Redirect URI: %s", config.RedirectURL)

	// Setup database
	db, err := setupDatabase()
	if err != nil {
		log.Fatalf("Failed to setup database: %v", err)
	}
	defer db.Close()

	log.Printf("Database setup completed")

	// Setup HTTP routes
	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/login", handleLogin)
	http.HandleFunc("/oauth/callback", handleOAuthCallback)
	http.HandleFunc("/download-emails", downloadEmailsHandler)
	http.HandleFunc("/enrich-emails", enrichEmailsHandler)
	http.HandleFunc("/enrich-emails-v1-2", enrichEmailsV1_2Handler)
	http.HandleFunc("/parse-signals", parseSignalsHandler)
	http.HandleFunc("/sql-parse-signals", sqlParseSignalsHandler)
	http.HandleFunc("/process-signals", processSignalsHandler)

	// Determine port
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server starting on :%s", port)
	log.Printf("Visit http://localhost:%s to get started", port)
	
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}