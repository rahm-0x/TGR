#streamlit_app.py
import streamlit as st
import pandas as pd
from pandas import DataFrame 
import psycopg2
from sqlalchemy import create_engine
import base64
import _thread
import hashlib
import plotly.express as px

# Set the page layout
st.set_page_config(layout='wide')

def autoplay_audio(file_path: str):
    with open(file_path, "rb") as f:
        data = f.read()
    b64 = base64.b64encode(data).decode()
    md = f"""<audio controls autoplay="true">
              <source src="data:audio/mp3;base64,{b64}" type="audio/mp3">
              </audio>"""
    st.markdown(md, unsafe_allow_html=True)

# Local cache dictionary
local_cache = {}

def hash_credentials(credentials):
    cred_str = ''.join(str(val) for val in credentials.values())
    return hashlib.md5(cred_str.encode()).hexdigest()

def calculate_sales(row: DataFrame, date_cols: list):
    # Reversing the date columns since the latest date is first
    reversed_date_cols = date_cols[::-1]
    
    # Calculate daily differences; the diff operation goes 'backwards' because of the reversed column order
    diffs = row[reversed_date_cols].diff(-1).fillna(0)
    
    # Remove negative sales because they might indicate returns or inventory adjustments
    diffs = diffs[diffs >= 0]
    
    # Sum up the positive sales to get the total sales for the given date range
    return diffs.sum()
    
def fetch_inventory_data(query):
    credentials = {
        'dbname': st.secrets["dbname"],
        'user': st.secrets["user"],
        'password': st.secrets["password"],
        'host': st.secrets["host"],
        'port': st.secrets["port"],
    }
    cred_hash = hash_credentials(credentials)
    cache_key = f"{cred_hash}:{hashlib.md5(query.encode()).hexdigest()}"
    
    if cache_key not in local_cache:
        engine = create_engine(f"postgresql+psycopg2://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['dbname']}")
        local_cache[cache_key] = pd.read_sql(query, engine)

    return local_cache[cache_key]

# Create a container to encapsulate all interactive elements
main_container = st.container()

def update_filter_options(df):
    return {
        "brands": ["ALL"] + df['brand'].unique().tolist(),
        "dispensaries": ["ALL"] + df['dispensary_name'].unique().tolist(),
        "categories": ["ALL"] + df['category'].unique().tolist(),
        "subcategories": ["ALL"] + df['subcategory'].unique().tolist(),
    }

with main_container:
    st.markdown(
        f'<a href="https://docs.google.com/spreadsheets/d/1KnKCSQRA_kROpu8PL1odabKVu5tzfBhHDBAdl96WaaE" target="_blank"><button style="margin: 2px; padding: 10px; background-color: #4CAF50; color: white;">Open Drops & Sell-Thru</button></a>',
        unsafe_allow_html=True,
    )
    if st.button("Fetch ALL"):
        query = 'SELECT * FROM public.t_dynamic_inventory_reverseddates'
        df = fetch_inventory_data(query)
        st.session_state['df'] = df  # Store df in session state
    elif st.button("Fetch TGC"):
        query = "SELECT * FROM public.t_dynamic_inventory_reverseddates WHERE brand like 'The Grower Circle'"
        df = fetch_inventory_data(query)
        st.session_state['df'] = df  # Store df in session state

    if 'df' in st.session_state:
        df = st.session_state['df']  # Retrieve df from session state
        recent_dates = sorted([col for col in df.columns if '-' in col], reverse=True)
    
        # Handle null values by replacing them with 0
        df[recent_dates] = df[recent_dates].fillna(0)
    
        # Assuming 'recent_dates' holds the sorted date columns with the latest date first
        if recent_dates:  # Check if recent_dates list is not empty
            latest_date = recent_dates[0]
        else:
            st.warning("No recent dates available.")
            latest_date = None  # Set latest_date to None if no recent dates are available

        # Calculate sales since yesterday
        if len(recent_dates) >= 2:
            df['Sales_Since_Yesterday'] = df[recent_dates[1]] - df[recent_dates[0]]
        else:
            df['Sales_Since_Yesterday'] = "NA"  # Fill with "NA" if data is insufficient

        # Convert 'Sales_Since_Yesterday' column to numeric type
        df['Sales_Since_Yesterday'] = pd.to_numeric(df['Sales_Since_Yesterday'], errors='coerce')

        # Filter out non-numeric values and then find the largest 5 values
        top_brands = df.groupby('name')['Sales_Since_Yesterday'].sum().dropna().nlargest(5)

       # Move 'Sales_Since_Yesterday' after 'price' column and before the first date column
        if 'Sales_Since_Yesterday' in df.columns:
            price_col_index = df.columns.get_loc("price") + 1  # Get index of 'price' column and add 1 to move after
            cols = df.columns.tolist()
            cols.insert(price_col_index, cols.pop(cols.index('Sales_Since_Yesterday')))
            df = df[cols]
        else:
            st.warning("Column 'Sales_Since_Yesterday' not found in DataFrame.")
                
        filter_options = update_filter_options(df)
        
        if latest_date is not None:
            df_in_stock = df[df[latest_date] > 0]  # Products in stock
            df_out_of_stock = df[df[latest_date] <= 0]  # Products out of stock or null
        else:
            df_in_stock = pd.DataFrame()  # Empty DataFrame
            df_out_of_stock = pd.DataFrame()  # Empty DataFrame


        # Filter widgets
        selected_brand = st.selectbox("Filter by Brand", filter_options["brands"], key='brand_filter')
        if selected_brand != "ALL":
            df = df[df['brand'] == selected_brand]
            filter_options = update_filter_options(df)

        selected_dispensary = st.selectbox("Filter by Dispensary", filter_options["dispensaries"], key='dispensary_filter')
        if selected_dispensary != "ALL":
            df = df[df['dispensary_name'] == selected_dispensary]
            filter_options = update_filter_options(df)

        selected_category = st.selectbox("Filter by Category", filter_options["categories"], key='category_filter')
        if selected_category != "ALL":
            df = df[df['category'] == selected_category]
            filter_options = update_filter_options(df)

        selected_subcategory = st.selectbox("Filter by Subcategory", filter_options["subcategories"], key='subcategory_filter')
        if selected_subcategory != "ALL":
            df = df[df['subcategory'] == selected_subcategory]
            filter_options = update_filter_options(df)

        search_term = st.text_input("Search", "")

        if search_term:
            df = df[df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)]


        st.title("Dispensary On-Hands & Sales Data")
        
        
        # Create two separate DataFrames
        if latest_date is not None:
            df_in_stock = df[df[latest_date] > 0]  # Products in stock
            df_out_of_stock = df[df[latest_date] <= 0]  # Products out of stock or null
        else:
            st.warning("No recent dates available.")
            df_in_stock = pd.DataFrame()  # Empty DataFrame
            df_out_of_stock = pd.DataFrame()  # Empty DataFrame


        df_out_of_stock = df[df[latest_date] <= 0] if latest_date is not None else pd.DataFrame()

        
        # Display these DataFrames as separate tables in Streamlit
        st.header("Products In Stock")
        st.dataframe(df_in_stock)
        
        st.header("Products Out Of Stock")
        st.dataframe(df_out_of_stock)

        st.markdown("* Sales since yesterday will be inaccurate for many dispensaries that cap the inventory levels in the data")

        # Dashboard Section
        st.title("Top Selling Products Dashboard")

        # Sales Calculation
        last_3_days = recent_dates[:3]
        last_7_days = recent_dates[:7]
        last_30_days = recent_dates[:30]
        
        df['Sales_Last_3_Days'] = df.apply(lambda row: calculate_sales(row, last_3_days), axis=1)
        df['Sales_Last_7_Days'] = df.apply(lambda row: calculate_sales(row, last_7_days), axis=1)
        df['Sales_Last_30_Days'] = df.apply(lambda row: calculate_sales(row, last_30_days), axis=1)

        # Dashboard Columns
        col1, col2 = st.columns(2)

        with col1:
            st.header("Short Term Performance")
            top_sold_yesterday = df[['name', 'Sales_Since_Yesterday']].sort_values('Sales_Since_Yesterday', ascending=False)
            top_sold_3_days = df[['name', 'Sales_Last_3_Days']].sort_values('Sales_Last_3_Days', ascending=False)

            st.subheader("Yesterday")
            st.dataframe(top_sold_yesterday)
            st.subheader("Last 3 Days")
            st.dataframe(top_sold_3_days)

        with col2:
            st.header("Long Term Performance")
            top_sold_7_days = df[['name', 'Sales_Last_7_Days']].sort_values('Sales_Last_7_Days', ascending=False)
            top_sold_30_days = df[['name', 'Sales_Last_30_Days']].sort_values('Sales_Last_30_Days', ascending=False)

            st.subheader("Last 7 Days")
            st.dataframe(top_sold_7_days)
            st.subheader("Last 30 Days")
            st.dataframe(top_sold_30_days)

        
            top_brands = df.groupby('name')['Sales_Since_Yesterday'].sum().nlargest(5)
            fig = px.pie(values=top_brands.values, names=top_brands.index, title='Top 5 Items by Sales Since Yesterday', labels={'value': 'Sales Since Yesterday'})
            fig.update_traces(textinfo='label+value')
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Please fetch data to proceed.")
