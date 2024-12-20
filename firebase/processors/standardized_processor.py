import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from firebase_admin import credentials, firestore, initialize_app
import time

# Initialize Firebase Admin SDK
cred = credentials.Certificate("/Users/phoenix/Desktop/TGR/firebase/thegrowersresource-1f2d7-firebase-adminsdk-hj18n-58a612a79d.json")
initialize_app(cred)
db = firestore.client()

# Fetch data from Firestore with pagination
def fetch_firestore_data_paginated(collection_name, page_size=500):
    """Fetch data from a Firestore collection in smaller batches using pagination."""
    collection_ref = db.collection(collection_name)
    documents = []
    last_doc = None

    while True:
        try:
            query = collection_ref.limit(page_size)
            if last_doc:
                query = query.start_after(last_doc)

            current_docs = list(query.stream())
            batch = [doc.to_dict() for doc in current_docs]

            if not batch:
                break

            documents.extend(batch)
            last_doc = current_docs[-1] if len(current_docs) == page_size else None
        except Exception as e:
            st.error(f"Error fetching Firestore data: {e}")
            break

    return pd.DataFrame(documents) if documents else pd.DataFrame()

# Fetch data from Firestore with retry logic
def fetch_firestore_data_with_retries(collection_name, retries=5, delay=1):
    """Fetch data from Firestore with retry logic for handling timeouts."""
    for attempt in range(retries):
        try:
            return fetch_firestore_data_paginated(collection_name)
        except Exception as e:
            st.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
            else:
                st.error(f"All retries failed for {collection_name}.")
                raise e

# Function to apply conditional formatting
def highlight_inventory(val):
    if pd.notna(val):
        if val < 15:
            return 'background-color: red'
        elif val < 25:
            return 'background-color: yellow'
    return ''

# Function to format inventory data as integers
def format_to_integer(df, cols):
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df

# Function to calculate sales, ignoring restocks
def calculate_sales(row, date_cols):
    reversed_date_cols = date_cols[::-1]
    diffs = row[reversed_date_cols].diff(-1).fillna(0)
    return diffs[diffs >= 0].sum()

# Main processor for standardized inventory data
def process_standardized_inventory_data():
    # Fetch data from Firestore using retries
    df_inventory = fetch_firestore_data_with_retries("standardized_inventory")

    if df_inventory.empty:
        st.error("No data loaded for Standardized Inventory.")
        return

    # Filter for Grower Circle and related brands
    tgc_brands = [
        "grower circle",
        "growers circle",
        "grower circle apparel",
        "the grower circle",
        "flight bites",
        "the grower circle"
    ]
    df_inventory = df_inventory[df_inventory['brand'].str.lower().str.strip().isin(tgc_brands)]

    # Pivot data for snapshot dates
    df_inventory['snapshot_date'] = pd.to_datetime(df_inventory['snapshot_date'], errors='coerce')
    df_pivoted = df_inventory.pivot_table(
        index=['dispensary_name', 'product_name', 'price', 'brand', 'category'],
        columns='snapshot_date',
        values='quantity',
        aggfunc='first'
    ).reset_index()

    # Identify recent date columns
    recent_dates = sorted([col for col in df_pivoted.columns if isinstance(col, pd.Timestamp)], reverse=True)
    if not recent_dates:
        st.error("No valid snapshot dates found. Please check the data.")
        return

    # Format inventory data as integers
    df_pivoted = format_to_integer(df_pivoted, recent_dates)

    # Filters
    unique_categories = df_pivoted['category'].dropna().unique().tolist()
    unique_categories.insert(0, "ALL")  # Add an "ALL" option
    selected_category = st.selectbox("Filter by Category", unique_categories, index=0, key="selected_category")
    if selected_category != "ALL":
        df_pivoted = df_pivoted[df_pivoted['category'] == selected_category]

    unique_dispensaries = df_pivoted['dispensary_name'].dropna().unique().tolist()
    unique_dispensaries.insert(0, "ALL")  # Add an "ALL" option
    selected_dispensary = st.selectbox("Filter by Dispensary", unique_dispensaries, index=0, key="selected_dispensary")
    if selected_dispensary != "ALL":
        df_pivoted = df_pivoted[df_pivoted['dispensary_name'] == selected_dispensary]

    search_term = st.text_input("Search by Product Name", key="search_product")
    if search_term:
        df_pivoted = df_pivoted[df_pivoted['product_name'].str.contains(search_term, case=False)]

    # Display data
    st.title("Standardized Inventory and Sales Data")
    columns_to_display = ['dispensary_name', 'product_name', 'price', 'brand', 'category'] + recent_dates
    styled_df = df_pivoted[columns_to_display].style.applymap(highlight_inventory, subset=recent_dates)
    st.dataframe(styled_df)

    # Calculate sales
    if len(recent_dates) >= 2:
        df_pivoted['Sales_Since_Yesterday'] = df_pivoted[recent_dates[1]] - df_pivoted[recent_dates[0]]
    else:
        df_pivoted['Sales_Since_Yesterday'] = "NA"

    df_pivoted['Sales_Last_3_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, recent_dates[:3]), axis=1)
    df_pivoted['Sales_Last_7_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, recent_dates[:7]), axis=1)
    df_pivoted['Sales_Last_30_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, recent_dates[:30]), axis=1)

    # Display performance sections
    col1, col2 = st.columns(2)

    with col1:
        st.header("Short Term Performance")
        top_sold_yesterday = df_pivoted[['product_name', 'Sales_Since_Yesterday']].sort_values('Sales_Since_Yesterday', ascending=False)
        st.subheader("Yesterday")
        st.dataframe(top_sold_yesterday)

        top_sold_7_days = df_pivoted[['product_name', 'Sales_Last_7_Days']].sort_values('Sales_Last_7_Days', ascending=False)
        st.subheader("Last 7 Days")
        st.dataframe(top_sold_7_days)

    with col2:
        st.header("Long Term Performance")
        top_sold_30_days = df_pivoted[['product_name', 'Sales_Last_30_Days']].sort_values('Sales_Last_30_Days', ascending=False)
        st.subheader("Last 30 Days")
        st.dataframe(top_sold_30_days)

if __name__ == "__main__":
    process_standardized_inventory_data()
