import os.path
import logging
import argparse

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set up loggingpy
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

def authenticate_gmail_api(credentials_file="/Users/darianhickman/Documents/Github/backteststoxx/client_secret_914016029840-24qpupahd54i01jt8kfvalmj2114kbh9.apps.googleusercontent.com.json", token_file="token.json", port=0):
    """
    Authenticates with Gmail API and returns a service object.
    
    Args:
        credentials_file (str): Path to OAuth2 credentials JSON file
        token_file (str): Path to store/retrieve access tokens
        port (int): Port for the local server for OAuth flow.
        
    Returns:
        googleapiclient.discovery.Resource: Gmail API service object or None if failed
    """
    creds = None
    
    # Check if credentials file exists
    if not os.path.exists(credentials_file):
        logger.error(f"Credentials file '{credentials_file}' not found.")
        logger.info("Please download your OAuth2 credentials from Google Cloud Console")
        return None
    
    # Load existing token if available
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
            logger.info("Loaded existing credentials")
        except Exception as e:
            logger.warning(f"Failed to load existing token: {e}")
            creds = None
    
    # Handle authentication
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                logger.info("Refreshing expired credentials...")
                creds.refresh(Request())
                logger.info("Credentials refreshed successfully")
            except Exception as e:
                logger.error(f"Failed to refresh credentials: {e}")
                creds = None
        
        if not creds:
            try:
                logger.info("Starting new OAuth flow...")
                flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
                
                # For automated environments, we use run_local_server() which handles the redirect flow.
                creds = flow.run_local_server(port=port)
                logger.info("New credentials obtained successfully")
                
            except Exception as e:
                logger.error(f"OAuth flow failed: {e}")
                return None

        # Save credentials for future use
        try:
            with open(token_file, "w") as token:
                token.write(creds.to_json())
            logger.info(f"Credentials saved to {token_file}")
        except Exception as e:
            logger.warning(f"Failed to save credentials: {e}")

    # Build and test the service
    try:
        service = build("gmail", "v1", credentials=creds)
        
        # Test the connection by fetching labels
        logger.info("Testing Gmail API connection...")
        results = service.users().labels().list(userId="me").execute()
        labels = results.get("labels", [])

        if not labels:
            logger.warning("No labels found in Gmail account")
        else:
            logger.info(f"Successfully connected! Found {len(labels)} labels:")
            for label in labels[:5]:  # Show first 5 labels
                print(f"  - {label['name']}")
            if len(labels) > 5:
                print(f"  ... and {len(labels) - 5} more")

        return service

    except HttpError as error:
        logger.error(f"Gmail API error: {error}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None

def main():
    """Main function to demonstrate the authentication."""
    parser = argparse.ArgumentParser(description="Gmail API Authentication Demo")
    parser.add_argument('--port', type=int, default=8080, help='Port for the local OAuth server.')
    args = parser.parse_args()

    print("Gmail API Authentication Demo")
    print("=" * 40)
    
    service = authenticate_gmail_api(port=args.port)
    
    if service:
        print("\n✅ Gmail API authenticated successfully!")
        print("You can now use the 'service' object to make Gmail API calls.")
        
        # Example: Get user profile
        try:
            profile = service.users().getProfile(userId="me").execute()
            print(f"📧 Email: {profile.get('emailAddress')}")
            print(f"📊 Total messages: {profile.get('messagesTotal', 'Unknown')}")
        except Exception as e:
            logger.warning(f"Could not fetch profile: {e}")
            
    else:
        print("\n❌ Gmail API authentication failed.")
        print("Please check your credentials and try again.")
    
    return service

if __name__ == "__main__":
    service = main()