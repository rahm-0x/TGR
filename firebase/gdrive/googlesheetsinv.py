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
    Fetch data from Firestore and update Google Sheets using a service account.
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
        df['snapshot_date'] = df['snapshot_date'].astype(str)

        # Remove duplicates to prevent pivot issues
        df = df.drop_duplicates(subset=['product_name', 'brand', 'dispensary_name', 'category', 'snapshot_date'])

        # Pivot the DataFrame to get quantities for each date
        pivoted_df = df.pivot(
            index=['product_name', 'brand', 'dispensary_name', 'category'],
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
        end_col = chr(64 + num_cols)  # Convert column number to letter
        update_range = f"{SHEET_NAME}!A1:{end_col}{num_rows + 1}"  # Start updating from cell A1

        # Authenticate using the service account file
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build("sheets", "v4", credentials=creds)

        # Update the sheet with new values
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

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    update_google_sheets()
