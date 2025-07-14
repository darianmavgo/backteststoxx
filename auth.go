package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

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

// loadCredentials loads OAuth credentials from file
func loadCredentials(credentialsFile string) (*oauth2.Config, error) {
	credBytes, err := os.ReadFile(credentialsFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read client secret file: %v", err)
	}

	// Print detailed credential information
	if _, err := printCredentialInfo(credBytes); err != nil {
		log.Printf("Warning: could not parse credential info: %v", err)
	}

	// Load OAuth configuration
	config, err := getConfigFromFile(credBytes)
	if err != nil {
		return nil, fmt.Errorf("unable to parse client secret file to config: %v", err)
	}

	return config, nil
}

// getConfigFromFile creates OAuth config from credentials file bytes
func getConfigFromFile(credBytes []byte) (*oauth2.Config, error) {
	var cred CredentialInfo
	if err := json.Unmarshal(credBytes, &cred); err != nil {
		return nil, err
	}

	// Determine redirect URI based on environment
	redirectURI := "http://localhost:8080/oauth/callback"
	if len(cred.Web.RedirectURIs) > 0 {
		// Use the first redirect URI from the credentials file
		redirectURI = cred.Web.RedirectURIs[0]
	}

	return &oauth2.Config{
		ClientID:     cred.Web.ClientID,
		ClientSecret: cred.Web.ClientSecret,
		RedirectURL:  redirectURI,
		Scopes:       []string{gmail.GmailReadonlyScope},
		Endpoint: oauth2.Endpoint{
			AuthURL:  cred.Web.AuthURI,
			TokenURL: cred.Web.TokenURI,
		},
	}, nil
}

// getTokenFromWeb opens browser for OAuth flow
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	log.Printf("Go to the following link in your browser: \n%v\n", authURL)

	fmt.Print("Enter the authorization code: ")
	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code: %v", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web: %v", err)
	}
	return tok
}

// tokenFromFile retrieves a token from a local file
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

// saveToken saves a token to a file path
func saveToken(path string, token *oauth2.Token) error {
	log.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
	return nil
}

// getGmailClient creates an authenticated Gmail client
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

// getGmailService creates an authenticated Gmail service
func getGmailService(ctx context.Context) (*gmail.Service, error) {
	client, err := getGmailClient(ctx)
	if err != nil {
		return nil, err
	}

	service, err := gmail.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("unable to create Gmail service: %v", err)
	}

	return service, nil
}

// OAuth handlers for web-based authentication
func handleLogin(w http.ResponseWriter, r *http.Request) {
	// Generate state token for security
	state := fmt.Sprintf("state-%d", time.Now().Unix())
	
	authURL := config.AuthCodeURL(state, oauth2.AccessTypeOffline)
	
	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

func handleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	// Parse the authorization code from the callback
	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "No authorization code received", http.StatusBadRequest)
		return
	}

	// Exchange the authorization code for an access token
	token, err := config.Exchange(context.Background(), code)
	if err != nil {
		log.Printf("Token exchange error: %v", err)
		http.Error(w, fmt.Sprintf("Failed to exchange token: %v", err), http.StatusInternalServerError)
		return
	}

	// Save the token
	if err := saveToken(tokenFile, token); err != nil {
		log.Printf("Failed to save token: %v", err)
		http.Error(w, fmt.Sprintf("Failed to save token: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("OAuth token saved successfully to %s", tokenFile)

	// Test the authentication by creating a Gmail service
	ctx := context.Background()
	service, err := getGmailService(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create Gmail service: %v", err), http.StatusInternalServerError)
		return
	}

	// Test by getting user profile
	profile, err := service.Users.GetProfile("me").Do()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get user profile: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully authenticated user: %s", profile.EmailAddress)

	// Get the redirect URI for display
	redirectURI := config.RedirectURL
	if redirectURI == "" {
		redirectURI = "http://localhost:8080/oauth/callback"
	}

	// Send success response
	html := fmt.Sprintf(`
	<!DOCTYPE html>
	<html>
		<head>
			<title>Authentication Successful</title>
			<style>
				body { font-family: Arial, sans-serif; margin: 40px; text-align: center; }
				.success { color: green; font-size: 18px; margin: 20px 0; }
				.info { color: #666; margin: 10px 0; }
				.button { display: inline-block; padding: 10px 20px; margin: 10px; 
						  background-color: #007cba; color: white; text-decoration: none; 
						  border-radius: 5px; }
				.button:hover { background-color: #005a8b; }
			</style>
		</head>
		<body>
			<h1>🎉 Authentication Successful!</h1>
			<div class="success">
				✅ Gmail API access has been granted and saved.<br/>
				✅ Authenticated as: %s
			</div>
			
			<div class="info">
				<p><strong>What happens next:</strong></p>
				<p>• Your OAuth token has been saved to: <code>%s</code></p>
				<p>• The application can now access Gmail API on your behalf</p>
				<p>• You can close this window and return to your application</p>
			</div>

			<div class="info">
				<p><strong>Configuration Details:</strong></p>
				<p>• Credentials file: <code>%s</code></p>
				<p>• Redirect URI: <code>%s</code></p>
			</div>

			<div>
				<a href="/" class="button">← Back to Home</a>
				<a href="/login" class="button">Try Login Again</a>
			</div>
		</body>
	</html>
	`, profile.EmailAddress, tokenFile, credentialsFile, redirectURI)

	fmt.Fprint(w, html)
}