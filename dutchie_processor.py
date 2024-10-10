import streamlit as st
import pandas as pd
import plotly.express as px
from filters import update_filter_options_dutchie
from datetime import datetime


pd.set_option("styler.render.max_elements", 5000000)

# Function to apply conditional formatting
def highlight_inventory(val):
    color = ''
    if pd.notna(val):
        if val < 15:
            color = 'background-color: red'
        elif val < 25:
            color = 'background-color: yellow'
    return color

# Function to remove decimal places from the dataframe
def format_to_integer(df, cols):
    df[cols] = df[cols].fillna(0).astype(int)  # Fill NaN with 0 and then convert to integers
    return df

# Function to calculate actual sales, ignoring restocks
def calculate_sales(row: pd.Series, date_cols: list):
    reversed_date_cols = date_cols[::-1]
    diffs = row[reversed_date_cols].diff(-1).fillna(0).infer_objects()
    sales = diffs[diffs >= 0]  # Ignore restocks, count sales (decreases)
    return sales.sum()

def process_dutchie_data():
    if 'df_dutchie' in st.session_state:
        df_dutchie = st.session_state['df_dutchie']

        # Identify date columns by checking for columns that resemble dates
        recent_dates = sorted([col for col in df_dutchie.columns if isinstance(col, str) and '-' in col], reverse=True)

        # Ensure the date columns are treated as numeric and fill any NaN values with 0
        df_dutchie[recent_dates] = df_dutchie[recent_dates].apply(pd.to_numeric, errors='coerce').fillna(0)

        # Format the inventory numbers as integers (remove decimal places)
        df_dutchie = format_to_integer(df_dutchie, recent_dates)

        # Calculate actual sales since yesterday
        if len(recent_dates) >= 2:
            df_dutchie['Sales_Since_Yesterday'] = df_dutchie.apply(lambda row: calculate_sales(row, recent_dates[:2]), axis=1)
        else:
            df_dutchie['Sales_Since_Yesterday'] = "NA"

        df_dutchie['Sales_Since_Yesterday'] = pd.to_numeric(df_dutchie['Sales_Since_Yesterday'], errors='coerce')

        # Calculate sales for the last 3, 7, and 30 days
        df_dutchie['Sales_Last_3_Days'] = df_dutchie.apply(lambda row: calculate_sales(row, recent_dates[:3]), axis=1)
        df_dutchie['Sales_Last_7_Days'] = df_dutchie.apply(lambda row: calculate_sales(row, recent_dates[:7]), axis=1)
        df_dutchie['Sales_Last_30_Days'] = df_dutchie.apply(lambda row: calculate_sales(row, recent_dates[:30]), axis=1)

        # Apply filters for Dutchie data
        filter_options_dutchie = update_filter_options_dutchie(df_dutchie)
        selected_brand_dutchie = st.selectbox("Filter by Brand (Dutchie)", filter_options_dutchie["brands"], key='brand_filter_dutchie')
        if selected_brand_dutchie != "ALL":
            df_dutchie = df_dutchie[df_dutchie['brand'] == selected_brand_dutchie]

        selected_dispensary_dutchie = st.selectbox("Filter by Dispensary (Dutchie)", filter_options_dutchie["dispensaries"], key='dispensary_filter_dutchie')
        if selected_dispensary_dutchie != "ALL":
            df_dutchie = df_dutchie[df_dutchie['dispensary_name'] == selected_dispensary_dutchie]

        selected_category_dutchie = st.selectbox("Filter by Category (Dutchie)", filter_options_dutchie["categories"], key='category_filter_dutchie')
        if selected_category_dutchie != "ALL":
            df_dutchie = df_dutchie[df_dutchie['category'] == selected_category_dutchie]

        # Search functionality
        search_term_dutchie = st.text_input("Search (Dutchie)", "")
        if search_term_dutchie:
            df_dutchie = df_dutchie[df_dutchie.apply(lambda row: row.astype(str).str.contains(search_term_dutchie, case=False).any(), axis=1)]

        st.title("Dutchie Dispensary On-Hands & Sales Data")

        # Exclude specific columns like sales metrics from the main display table
        columns_to_display = [col for col in df_dutchie.columns if col not in ['Sales_Last_3_Days', 'Sales_Last_7_Days', 'Sales_Last_30_Days']]

        # Apply conditional formatting to inventory values
        styled_df = df_dutchie[columns_to_display].style.applymap(highlight_inventory, subset=recent_dates)

        # Display the dataframe with conditional formatting
        st.dataframe(styled_df)

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
