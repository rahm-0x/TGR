import os
import json
import toml
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def get_credentials():
    credentials = None
    if os.path.exists("token.json"):
        credentials = Credentials.from_authorized_user_file("token.json", SCOPES)
    
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("client_secret_554969294371-g5dpr5crn6bdvf1uv0tg4jcvnj5j97ui.apps.googleusercontent.com.json", SCOPES)
            credentials = flow.run_local_server(port=8080)
        
        with open("token.json", "w") as token:
            token.write(credentials.to_json())
            
    return credentials

if __name__ == '__main__':
    get_credentials()
