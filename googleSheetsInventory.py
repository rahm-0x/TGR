# googleSheetsInventoryUpdate.py
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import toml  # Import the toml module

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1jHBz2zzvIO8KcWscrqq43wqV74DLiq1Jv1DjB2nd1DE"
SHEET_NAME = "InventoryData"

def get_credentials():
    credentials = None

    if os.path.exists("token.json"):
        credentials = Credentials.from_authorized_user_file("token.json", SCOPES)
    
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            credentials = flow.run_local_server(port=8080)
        
        with open("token.json", "w") as token:
            token.write(credentials.to_json())
            
    return credentials

def colnum_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def main():
    try:
        # Build the Google Sheets API client
        sheets_service = build('sheets', 'v4', credentials=get_credentials())
        sheet = sheets_service.spreadsheets()

        # Load the .toml credentials
        secrets = toml.load("secrets.toml")

        # Connection string for SQLAlchemy
        engine = create_engine(f"postgresql+psycopg2://{secrets['user']}:{secrets['password']}@{secrets['host']}:{secrets['port']}/{secrets['dbname']}")

        # Execute the SQL query and store the result in a DataFrame
        query = "SELECT * FROM public.dispensary_inventory_snapshot WHERE brand = 'The Grower Circle';"
        df = pd.read_sql_query(query, con=engine)

        # Debug: Print the number of rows retrieved
        print(f"Number of rows retrieved: {df.shape[0]}")

        # Replace NaN values with an empty string
        df.fillna('', inplace=True)

        # Debug: Print DataFrame info
        print("DataFrame Info:")
        print(df.info())
        print("DataFrame Head:")
        print(df.head())

        # Rename 'snapshot_timestamp' to 'snapshot_date' to match the expected column name
        df.rename(columns={'snapshot_timestamp': 'snapshot_date'}, inplace=True)

        # Convert 'snapshot_date' to string for compatibility
        df['snapshot_date'] = df['snapshot_date'].astype(str)

        # Create a DataFrame with unique dates from the snapshot_date column
        unique_dates = df['snapshot_date'].unique()
        new_columns = ['snapshot_date'] + [str(date) for date in unique_dates]

        # Pivot the DataFrame to get quantities for each date
        pivoted_df = df.pivot(index=['name', 'brand', 'dispensary_name'], columns='snapshot_date', values='quantity').reset_index()
        pivoted_df.columns.name = None  # Remove the column name for cleaner output

        # Merge the pivoted DataFrame with the original DataFrame on name, brand, and dispensary_name
        merged_df = df.merge(pivoted_df, on=['name', 'brand', 'dispensary_name'], how='left')

        # Prepare data with headers for the update
        data_with_headers = [merged_df.columns.tolist()] + merged_df.astype(str).values.tolist()

        # Determine the number of rows and columns in the DataFrame
        num_rows, num_cols = merged_df.shape

        # Calculate the range for updating starting from cell A1
        end_col = colnum_string(num_cols)
        update_range = f"{SHEET_NAME}!A1:{end_col}{num_rows + 1}"  # Start updating from cell A1

        # Print the range and data for debugging
        print("Update range:", update_range)
        print("Data headers:", data_with_headers[0])
        print("Data sample:", data_with_headers[1:3])  # Print first few rows to check

        # Check if the sheet/tab exists
        sheets_metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheet_names = [s['properties']['title'] for s in sheets_metadata.get('sheets', '')]
        if SHEET_NAME not in sheet_names:
            raise ValueError(f"Sheet '{SHEET_NAME}' does not exist in the spreadsheet.")

        # Update with new values
        update_request = {
            "spreadsheetId": SPREADSHEET_ID,
            "valueInputOption": "RAW",
            "data": [{
                "range": update_range,
                "values": data_with_headers
            }]
        }
        sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=update_request).execute()

    except Exception as e:
        print(f'An error occurred: {e}')

if __name__ == '__main__':
    main()
