import streamlit as st
import pandas as pd
import time
from datetime import datetime, date
from firebase_admin import credentials, firestore, initialize_app
import os

# Define relative path for credentials with dynamic handling
FIREBASE_CREDENTIALS_PATH = os.path.join(
    os.path.dirname(__file__), ".secrets", "thegrowersresource-1f2d7-firebase-adminsdk-hj18n-7101b02dc4.json"
)

if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
    raise FileNotFoundError(f"Firebase credentials file not found at {FIREBASE_CREDENTIALS_PATH}")

cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
initialize_app(cred)
db = firestore.client()

def fetch_firestore_data_paginated(collection_name, page_size=100):
    documents = []
    last_doc = None
    while True:
        query = db.collection(collection_name).limit(page_size)
        if last_doc:
            query = query.start_after(last_doc)
        current_docs = list(query.stream())
        batch = [doc.to_dict() for doc in current_docs]
        if not batch:
            break
        documents.extend(batch)
        last_doc = current_docs[-1] if len(current_docs) == page_size else None
        if len(documents) > 1000:
            break
    return pd.DataFrame(documents) if documents else pd.DataFrame()

def fetch_firestore_data_with_retries(collection_name, retries=5, delay=1):
    for attempt in range(retries):
        try:
            return fetch_firestore_data_paginated(collection_name)
        except Exception as e:
            st.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                st.error(f"All retries failed for {collection_name}.")
                raise e

def highlight_inventory(val):
    if pd.notna(val):
        if val < 15:
            return 'background-color: red'
        elif val < 25:
            return 'background-color: yellow'
    return ''

def format_to_integer(df, cols):
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df

def process_standardized_inventory_data():
    df_inventory = fetch_firestore_data_with_retries("TGC_Standardized")
    if df_inventory.empty:
        st.error("No data loaded for Standardized Inventory.")
        return

    # Filter for Grower Circle brands
    tgc_brands = [
        "grower circle", "growers circle", "grower circle apparel",
        "the grower circle", "flight bites", "the grower circle"
    ]
    df_inventory = df_inventory[df_inventory['brand'].str.lower().str.strip().isin(tgc_brands)]
    
    # Convert snapshot_time to a string date in YYYY-MM-DD format
    df_inventory['snapshot_date_str'] = pd.to_datetime(df_inventory['snapshot_time']).dt.date.astype(str)
    
    # Ensure 'quantity' is numeric
    df_inventory['quantity'] = pd.to_numeric(df_inventory['quantity'], errors='coerce').fillna(0)
    
    # Group by relevant fields, including snapshot_date_str
    df_grouped = df_inventory.groupby([
        'dispensary_name', 'product_name', 'price', 'brand', 'category', 'snapshot_date_str'
    ]).agg({'quantity': 'sum'}).reset_index()
    
    # Pivot the table so that each snapshot_date_str becomes a column
    df_pivoted = df_grouped.pivot(
        index=['dispensary_name', 'product_name', 'price', 'brand', 'category'],
        columns='snapshot_date_str',
        values='quantity'
    ).reset_index()
    
    # Fill any missing values with 0
    df_pivoted.fillna(0, inplace=True)
    
    # Identify date columns (which are now strings in the format YYYY-MM-DD)
    date_columns = [col for col in df_pivoted.columns if isinstance(col, str) and '-' in col]
    
    # Sort date columns in descending order (newest first)
    date_columns = sorted(date_columns, reverse=True)
    
    # Convert the pivoted date columns to integers
    df_pivoted = format_to_integer(df_pivoted, date_columns)
    
    # Apply filters if selected
    unique_categories = df_pivoted['category'].dropna().unique().tolist()
    unique_categories.insert(0, "ALL")
    selected_category = st.selectbox("Filter by Category", unique_categories, index=0, key="selected_category")
    if selected_category != "ALL":
        df_pivoted = df_pivoted[df_pivoted['category'] == selected_category]
    
    unique_dispensaries = df_pivoted['dispensary_name'].dropna().unique().tolist()
    unique_dispensaries.insert(0, "ALL")
    selected_dispensary = st.selectbox("Filter by Dispensary", unique_dispensaries, index=0, key="selected_dispensary")
    if selected_dispensary != "ALL":
        df_pivoted = df_pivoted[df_pivoted['dispensary_name'] == selected_dispensary]
    
    search_term = st.text_input("Search by Product Name", key="search_product")
    if search_term:
        df_pivoted = df_pivoted[df_pivoted['product_name'].str.contains(search_term, case=False)]
    
    st.title("Standardized Inventory and Sales Data")
    columns_to_display = ['dispensary_name', 'product_name', 'price', 'brand', 'category'] + date_columns
    styled_df = df_pivoted[columns_to_display].style.applymap(highlight_inventory, subset=date_columns)
    st.dataframe(styled_df)
    
    # Calculate sales metrics using the descending-sorted date columns (newest first)
    if len(date_columns) >= 2:
        df_pivoted['Sales_Since_Yesterday'] = df_pivoted[date_columns[0]] - df_pivoted[date_columns[1]]
        df_pivoted['Sales_Since_Yesterday'] = df_pivoted['Sales_Since_Yesterday'].astype(int)
    else:
        df_pivoted['Sales_Since_Yesterday'] = "NA"
    
    if len(date_columns) >= 3:
        df_pivoted['Sales_Last_3_Days'] = df_pivoted[date_columns[:3]].sum(axis=1)
        df_pivoted['Sales_Last_3_Days'] = df_pivoted['Sales_Last_3_Days'].astype(int)
    else:
        df_pivoted['Sales_Last_3_Days'] = "NA"
    
    if len(date_columns) >= 7:
        df_pivoted['Sales_Last_7_Days'] = df_pivoted[date_columns[:7]].sum(axis=1)
        df_pivoted['Sales_Last_7_Days'] = df_pivoted['Sales_Last_7_Days'].astype(int)
    else:
        df_pivoted['Sales_Last_7_Days'] = "NA"
    
    col1, col2 = st.columns(2)
    with col1:
        st.header("Short Term Performance")
        top_sold_yesterday = df_pivoted[['product_name', 'Sales_Since_Yesterday']].sort_values('Sales_Since_Yesterday', ascending=False)
        st.subheader("Yesterday")
        st.dataframe(top_sold_yesterday)
    
        top_sold_3_days = df_pivoted[['product_name', 'Sales_Last_3_Days']].sort_values('Sales_Last_3_Days', ascending=False)
        st.subheader("Last 3 Days")
        st.dataframe(top_sold_3_days)
    
    with col2:
        st.header("Long Term Performance")
        top_sold_7_days = df_pivoted[['product_name', 'Sales_Last_7_Days']].sort_values('Sales_Last_7_Days', ascending=False)
        st.subheader("Last 7 Days")
        st.dataframe(top_sold_7_days)

if __name__ == "__main__":
    process_standardized_inventory_data()
