# googleSheetsInventoryUpdate.py
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import toml

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
        query = """
            SELECT 
                dispensary_name, 
                product_name AS name, 
                brand, 
                category, 
                snapshot_date, 
                quantity 
            FROM public.standardized_inventory
            WHERE "brand" ILIKE 'grower circle'
               OR "brand" ILIKE 'growers circle'
               OR "brand" ILIKE 'grower circle apparel'
               OR "brand" ILIKE 'the grower circle'
               OR "brand" ILIKE 'flight bites'
               OR "brand" ILIKE 'the grower circle';
        """
        print("Executing SQL query...")
        df = pd.read_sql_query(query, con=engine)

        # Debug: Verify the query results
        print(f"Number of rows retrieved after filtering: {df.shape[0]}")
        print("Sample rows:")
        print(df.head())

        # Replace NaN values with an empty string
        df.fillna('', inplace=True)

        # Debug: Print DataFrame info
        print("DataFrame Info:")
        print(df.info())

        # Ensure 'snapshot_date' is a string for compatibility
        df['snapshot_date'] = df['snapshot_date'].astype(str)

        # Remove duplicates to prevent pivot issues
        df = df.drop_duplicates(subset=['name', 'brand', 'dispensary_name', 'category', 'snapshot_date'])

        # Pivot the DataFrame to get quantities for each date
        pivoted_df = df.pivot(
            index=['name', 'brand', 'dispensary_name', 'category'],
            columns='snapshot_date',
            values='quantity'
        ).reset_index()

        # Fill NaN with 0 in the pivoted DataFrame
        pivoted_df.fillna(0, inplace=True)
        pivoted_df.columns.name = None  # Remove the column name for cleaner output

        # Prepare data with headers for the update
        data_with_headers = [pivoted_df.columns.tolist()] + pivoted_df.astype(str).values.tolist()

        # Determine the number of rows and columns in the DataFrame
        num_rows, num_cols = pivoted_df.shape

        # Calculate the range for updating starting from cell A1
        end_col = colnum_string(num_cols)
        update_range = f"{SHEET_NAME}!A1:{end_col}{num_rows + 1}"  # Start updating from cell A1

        # Debug: Print the range and data for verification
        print("Update range:", update_range)
        print("Data headers:", data_with_headers[0])
        print("Data sample:", data_with_headers[1:3])

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
