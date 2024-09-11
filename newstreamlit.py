import streamlit as st
import pandas as pd
from pandas import DataFrame
import psycopg2
from sqlalchemy import create_engine
import base64
import hashlib
import plotly.express as px

# Set the page layout
st.set_page_config(layout='wide')

# Local cache dictionary
local_cache = {}

def hash_credentials(credentials):
    cred_str = ''.join(str(val) for val in credentials.values())
    return hashlib.md5(cred_str.encode()).hexdigest()

def calculate_sales(row: DataFrame, date_cols: list):
    reversed_date_cols = date_cols[::-1]
    diffs = row[reversed_date_cols].diff(-1).fillna(0).infer_objects()
    diffs = diffs[diffs >= 0]
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
        try:
            engine = create_engine(f"postgresql+psycopg2://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['dbname']}")
            with engine.connect() as connection:
                local_cache[cache_key] = pd.read_sql(query, connection)
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error

    return local_cache[cache_key]

# Filter function for Dutchie
def update_filter_options_dutchie(df):
    return {
        "brands": ["ALL"] + df['brand'].unique().tolist(),
        "dispensaries": ["ALL"] + df['dispensary_name'].unique().tolist(),
        "categories": ["ALL"] + df['category'].unique().tolist(),
        "subcategories": ["ALL"] + df['subcategory'].unique().tolist(),
    }

# Filter function for Typesense
def update_filter_options_typesense(df):
    return {
        "brands": ["ALL"] + df['Brand'].unique().tolist(),
        "locations": ["ALL"] + df['Location'].unique().tolist(),
        "categories": ["ALL"] + df['Category'].unique().tolist(),
        "subcategories": ["ALL"] + df['subcategoryName'].unique().tolist(),
    }

# Create a container to encapsulate all interactive elements
main_container = st.container()

with main_container:
    st.markdown(
        f'<a href="https://docs.google.com/spreadsheets/d/1jHBz2zzvIO8KcWscrqq43wqV74DLiq1Jv1DjB2nd1DE/edit?usp=sharing" target="_blank"><button style="margin: 2px; padding: 10px; background-color: #4CAF50; color: white;">Open Drops & Sell-Thru</button></a>',
        unsafe_allow_html=True,
    )

    # Button for Fetching ALL data (Dutchie)
    if st.button("Fetch ALL (Dutchie)"):
        query = 'SELECT * FROM public.t_dynamic_inventory_reverseddates'
        df_dutchie = fetch_inventory_data(query)
        st.session_state['df_dutchie'] = df_dutchie  # Store df in session state

    # Button for Fetching TGC data (Dutchie)
    elif st.button("Fetch TGC (Dutchie)"):
        query = "SELECT * FROM public.t_dynamic_inventory_reverseddates WHERE brand like 'The Grower Circle'"
        df_dutchie = fetch_inventory_data(query)
        st.session_state['df_dutchie'] = df_dutchie  # Store df in session state

    # Button for Fetching Typesense data
    elif st.button("Fetch Typesense Data"):
        query = 'SELECT * FROM public.typesense_table'
        df_typesense = fetch_inventory_data(query)
        st.session_state['df_typesense'] = df_typesense

    # Fetching Typesense data for TGC
    elif st.button("Fetch TGC from Typesense"):
        query = """
        SELECT * FROM public.typesense_table
        WHERE "Brand" ILIKE '%flight%'
           OR "Brand" ILIKE '%Grower Circle%'
           OR "Brand" ILIKE '%Growers Circle%'
           OR "Brand" ILIKE '%GROWERS CIRCLE%'
        """
        df_typesense_tgc = fetch_inventory_data(query)
        st.session_state['df_typesense'] = df_typesense_tgc

    # Dutchie Data Processing
    if 'df_dutchie' in st.session_state:
        df_dutchie = st.session_state['df_dutchie']  # Retrieve df from session state

        # Dynamically find the most recent date columns (snapshot dates)
        recent_dates = sorted([col for col in df_dutchie.columns if '-' in col], reverse=True)

        # Handle null values by replacing them with 0
        df_dutchie[recent_dates] = df_dutchie[recent_dates].fillna(0)

        # Calculate sales since yesterday
        if len(recent_dates) >= 2:
            df_dutchie['Sales_Since_Yesterday'] = df_dutchie[recent_dates[1]] - df_dutchie[recent_dates[0]]
        else:
            df_dutchie['Sales_Since_Yesterday'] = "NA"

        df_dutchie['Sales_Since_Yesterday'] = pd.to_numeric(df_dutchie['Sales_Since_Yesterday'], errors='coerce')

        # Calculate sales for Last 3, 7, and 30 Days
        last_3_days = recent_dates[:3]
        last_7_days = recent_dates[:7]
        last_30_days = recent_dates[:30]

        df_dutchie['Sales_Last_3_Days'] = df_dutchie.apply(lambda row: calculate_sales(row, last_3_days), axis=1)
        df_dutchie['Sales_Last_7_Days'] = df_dutchie.apply(lambda row: calculate_sales(row, last_7_days), axis=1)
        df_dutchie['Sales_Last_30_Days'] = df_dutchie.apply(lambda row: calculate_sales(row, last_30_days), axis=1)

        # Updating the filter options for Dutchie
        filter_options_dutchie = update_filter_options_dutchie(df_dutchie)

        # Filters for brands, dispensaries, categories, subcategories, and search term
        selected_brand_dutchie = st.selectbox("Filter by Brand (Dutchie)", filter_options_dutchie["brands"], key='brand_filter_dutchie')
        if selected_brand_dutchie != "ALL":
            df_dutchie = df_dutchie[df_dutchie['brand'] == selected_brand_dutchie]

        selected_dispensary_dutchie = st.selectbox("Filter by Dispensary (Dutchie)", filter_options_dutchie["dispensaries"], key='dispensary_filter_dutchie')
        if selected_dispensary_dutchie != "ALL":
            df_dutchie = df_dutchie[df_dutchie['dispensary_name'] == selected_dispensary_dutchie]

        selected_category_dutchie = st.selectbox("Filter by Category (Dutchie)", filter_options_dutchie["categories"], key='category_filter_dutchie')
        if selected_category_dutchie != "ALL":
            df_dutchie = df_dutchie[df_dutchie['category'] == selected_category_dutchie]

        selected_subcategory_dutchie = st.selectbox("Filter by Subcategory (Dutchie)", filter_options_dutchie["subcategories"], key='subcategory_filter_dutchie')
        if selected_subcategory_dutchie != "ALL":
            df_dutchie = df_dutchie[df_dutchie['subcategory'] == selected_subcategory_dutchie]

        search_term_dutchie = st.text_input("Search (Dutchie)", "")

        if search_term_dutchie:
            df_dutchie = df_dutchie[df_dutchie.apply(lambda row: row.astype(str).str.contains(search_term_dutchie, case=False).any(), axis=1)]

        # Remove the "Sales_Last_3_Days", "Sales_Last_7_Days", and "Sales_Last_30_Days" columns from the main table
        columns_to_display = [col for col in df_dutchie.columns if col not in ['Sales_Last_3_Days', 'Sales_Last_7_Days', 'Sales_Last_30_Days']]

        st.title("Dutchie Dispensary On-Hands & Sales Data")
        st.dataframe(df_dutchie[columns_to_display])
        

        # Display sales data and pie chart
        col1, col2 = st.columns(2)

        with col1:
            st.header("Short Term Performance")
            top_sold_yesterday = df_dutchie[['name', 'Sales_Since_Yesterday']].sort_values('Sales_Since_Yesterday', ascending=False)
            top_sold_3_days = df_dutchie[['name', 'Sales_Last_3_Days']].sort_values('Sales_Last_3_Days', ascending=False)
            out_of_stock = df_dutchie[df_dutchie[recent_dates[0]] == 0][['dispensary_name', 'name', 'category', 'price', recent_dates[0]]]

        
            st.subheader("Yesterday")
            st.dataframe(top_sold_yesterday)
            st.subheader("Last 3 Days")
            st.dataframe(top_sold_3_days)
            st.header("Out of Stock Products")
            st.dataframe(out_of_stock.rename(columns={recent_dates[0]: 'Available Quantity'}))

        with col2:
            st.header("Long Term Performance")
            top_sold_7_days = df_dutchie[['name', 'Sales_Last_7_Days']].sort_values('Sales_Last_7_Days', ascending=False)
            top_sold_30_days = df_dutchie[['name', 'Sales_Last_30_Days']].sort_values('Sales_Last_30_Days', ascending=False)

            st.subheader("Last 7 Days")
            st.dataframe(top_sold_7_days)
            st.subheader("Last 30 Days")
            st.dataframe(top_sold_30_days)

            

            # Pie chart for top brands by sales since yesterday
            top_brands = df_dutchie.groupby('name')['Sales_Since_Yesterday'].sum().nlargest(5)
            fig = px.pie(values=top_brands.values, names=top_brands.index, title='Top 5 Items by Sales Since Yesterday', labels={'value': 'Sales Since Yesterday'})
            fig.update_traces(textinfo='label+value')
            st.plotly_chart(fig, use_container_width=True)



    # Typesense Data Processing
    if 'df_typesense' in st.session_state:
        df_typesense = st.session_state['df_typesense']  # Retrieve df from session state

        # Updating the filter options for Typesense
        filter_options_typesense = update_filter_options_typesense(df_typesense)

        # Filters for brands, locations, categories, subcategories, and search term
        selected_brand_typesense = st.selectbox("Filter by Brand (Typesense)", filter_options_typesense["brands"], key='brand_filter_typesense')
        if selected_brand_typesense != "ALL":
            df_typesense = df_typesense[df_typesense['Brand'] == selected_brand_typesense]

        selected_location_typesense = st.selectbox("Filter by Location (Typesense)", filter_options_typesense["locations"], key='location_filter_typesense')
        if selected_location_typesense != "ALL":
            df_typesense = df_typesense[df_typesense['Location'] == selected_location_typesense]

        selected_category_typesense = st.selectbox("Filter by Category (Typesense)", filter_options_typesense["categories"], key='category_filter_typesense')
        if selected_category_typesense != "ALL":
            df_typesense = df_typesense[df_typesense['Category'] == selected_category_typesense]

        selected_subcategory_typesense = st.selectbox("Filter by Subcategory (Typesense)", filter_options_typesense["subcategories"], key='subcategory_filter_typesense')
        if selected_subcategory_typesense != "ALL":
            df_typesense = df_typesense[df_typesense['subcategoryName'] == selected_subcategory_typesense]

        search_term_typesense = st.text_input("Search (Typesense)", key='typesense_search')

        if search_term_typesense:
            df_typesense = df_typesense[df_typesense.apply(lambda row: row.astype(str).str.contains(search_term_typesense, case=False).any(), axis=1)]

        st.title("Typesense Data")
        st.dataframe(df_typesense)
