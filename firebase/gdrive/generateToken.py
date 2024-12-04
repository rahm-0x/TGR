import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Define the scopes required for Google Sheets API
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_credentials():
    """
    Fetch or generate Google OAuth2 credentials for accessing the Google Sheets API.
    Ensures token.json exists and is valid, otherwise prompts for re-authentication.
    """
    credentials = None

    # Check if the token.json file exists
    if os.path.exists("token.json"):
        try:
            credentials = Credentials.from_authorized_user_file("token.json", SCOPES)
        except Exception as e:
            print(f"Error reading token.json: {e}")
    
    # Refresh credentials if expired, or authenticate if no valid credentials
    if not credentials or not credentials.valid:
        try:
            if credentials and credentials.expired and credentials.refresh_token:
                print("Refreshing expired credentials...")
                credentials.refresh(Request())
            else:
                print("No valid credentials found. Starting OAuth2 flow...")
                # Provide the path to your client_secret.json file
                flow = InstalledAppFlow.from_client_secrets_file(
                    "/Users/phoenix/Desktop/TGR/firebase/gdrive/client_secret_708823886373-91ehsmonsldhcvgtnmet2v9lj2v8asvr.apps.googleusercontent.com.json", 
                    SCOPES
                )
                # Run local server for authentication
                credentials = flow.run_local_server(port=8080)
            
            # Save the credentials to token.json for future use
            with open("token.json", "w") as token:
                token.write(credentials.to_json())
                print("Credentials saved to token.json.")
        except FileNotFoundError:
            print("Error: client_secret.json file not found. Ensure the correct file path is set.")
            raise
        except Exception as e:
            print(f"An error occurred during the OAuth flow: {e}")
            raise

    return credentials

if __name__ == '__main__':
    print("Starting Google OAuth2 authentication process...")
    creds = get_credentials()
    if creds and creds.valid:
        print("Authentication successful! Credentials are valid.")
    else:
        print("Authentication failed. Please check the logs above for details.")
