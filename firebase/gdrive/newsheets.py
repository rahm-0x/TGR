import os
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from firebase_admin import credentials as firebase_credentials, firestore, initialize_app

# Google Sheets API configuration
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1jHBz2zzvIO8KcWscrqq43wqV74DLiq1Jv1DjB2nd1DE"
SHEET_NAME = "InventoryData"

# Path to your Google Cloud Service Account JSON key file
SERVICE_ACCOUNT_FILE = "/Users/phoenix/Desktop/TGR/firebase/gdrive/thegrowersresource-1f2d7-17228b16128b.json"

# Firebase initialization
FIREBASE_CREDENTIALS_FILE = "/Users/phoenix/Desktop/TGR/firebase/thegrowersresource-1f2d7-firebase-adminsdk-hj18n-58a612a79d.json"
firebase_cred = firebase_credentials.Certificate(FIREBASE_CREDENTIALS_FILE)
initialize_app(firebase_cred)
db = firestore.client()

def fetch_firestore_data():
    """
    Fetch data from Firestore for Grower Circle brands.
    """
    tgc_brands = [
        "grower circle",
        "growers circle",
        "grower circle apparel",
        "the grower circle",
        "flight bites",
        "the grower circle",
    ]
    docs = db.collection("standardized_inventory").stream()
    data = [doc.to_dict() for doc in docs]
    df = pd.DataFrame(data)

    if not df.empty:
        # Filter for Grower Circle brands
        df = df[df['brand'].str.lower().str.strip().isin(tgc_brands)]
    return df

def update_google_sheets():
    """
    Fetch data from Firestore and update Google Sheets with highlighting and sorted dates.
    """
    try:
        # Fetch Firestore data
        print("Fetching data from Firestore...")
        df = fetch_firestore_data()

        if df.empty:
            print("No data retrieved from Firestore.")
            return

        # Debug: Verify the query results
        print(f"Number of rows retrieved after filtering: {df.shape[0]}")
        print("Sample rows:")
        print(df.head())

        # Replace NaN values with an empty string
        df.fillna('', inplace=True)

        # Ensure 'snapshot_date' is a string for compatibility
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.date.astype(str)

        # Remove duplicates to prevent pivot issues
        df = df.drop_duplicates(subset=['product_name', 'brand', 'dispensary_name', 'category', 'snapshot_date'])

        # Pivot the DataFrame to get quantities for each date
        pivoted_df = df.pivot(
            index=['product_name', 'brand', 'dispensary_name', 'category'],
            columns='snapshot_date',
            values='quantity'
        ).reset_index()

        # Sort snapshot dates in descending order
        snapshot_columns = sorted(
            [col for col in pivoted_df.columns if isinstance(col, str) and '-' in col],
            reverse=True
        )
        ordered_columns = ['product_name', 'brand', 'dispensary_name', 'category'] + snapshot_columns
        pivoted_df = pivoted_df[ordered_columns]

        # Fill NaN with 0 in the pivoted DataFrame
        pivoted_df.fillna(0, inplace=True)
        pivoted_df.columns.name = None  # Remove the column name for cleaner output

        # Prepare data with headers for the update
        data_with_headers = [pivoted_df.columns.tolist()] + pivoted_df.astype(str).values.tolist()

        # Determine the number of rows and columns in the DataFrame
        num_rows, num_cols = pivoted_df.shape

        # Authenticate using the service account file
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build("sheets", "v4", credentials=creds)

        # Update the sheet with new values
        update_range = f"{SHEET_NAME}!A1"
        body = {
            "range": update_range,
            "values": data_with_headers,
            "majorDimension": "ROWS"
        }
        print("Updating Google Sheets...")
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=update_range,
            valueInputOption="RAW",
            body=body
        ).execute()

        print(f"{result.get('updatedCells')} cells updated successfully.")

        # Apply conditional formatting for highlighting
        print("Applying conditional formatting...")
        conditional_formats = {
            "requests": [
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [
                                {
                                    "sheetId": 0,  # Replace with the actual sheet ID
                                    "startRowIndex": 1,
                                    "endRowIndex": num_rows + 1,
                                    "startColumnIndex": 4,
                                    "endColumnIndex": num_cols + 1
                                }
                            ],
                            "booleanRule": {
                                "condition": {
                                    "type": "NUMBER_LESS",
                                    "values": [{"userEnteredValue": "10"}]
                                },
                                "format": {
                                    "backgroundColor": {"red": 1, "green": 0, "blue": 0}
                                }
                            }
                        },
                        "index": 0
                    }
                },
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [
                                {
                                    "sheetId": 0,  # Replace with the actual sheet ID
                                    "startRowIndex": 1,
                                    "endRowIndex": num_rows + 1,
                                    "startColumnIndex": 4,
                                    "endColumnIndex": num_cols + 1
                                }
                            ],
                            "booleanRule": {
                                "condition": {
                                    "type": "NUMBER_LESS",
                                    "values": [{"userEnteredValue": "15"}]
                                },
                                "format": {
                                    "backgroundColor": {"red": 1, "green": 1, "blue": 0}
                                }
                            }
                        },
                        "index": 1
                    }
                }
            ]
        }
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=conditional_formats
        ).execute()

        print("Conditional formatting applied successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    update_google_sheets()
