import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from filters import update_filter_options_dutchie

pd.set_option("styler.render.max_elements", 5000000)

# Function to apply conditional formatting
def highlight_inventory(val):
    if pd.notna(val):
        if val < 10:
            return 'background-color: red'
        elif val < 15:
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

# Main Processor
def process_iheartjane_data():
    if 'df_iheartjane' not in st.session_state:
        st.error("No iHeartJane data loaded.")
        return

    df_iheartjane = st.session_state['df_iheartjane']

    # Identify snapshot_time and pivot the data
    df_iheartjane['snapshot_time'] = pd.to_datetime(df_iheartjane['snapshot_time'], errors='coerce')
    df_pivoted = df_iheartjane.pivot_table(
        index=['store_name', 'product_name', 'type', 'price', 'brand'],
        columns='snapshot_time',
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

    # Manage filters using session state
    filter_options = update_filter_options_dutchie(df_iheartjane)

    # Persistent filter values
    selected_brand = st.selectbox(
        "Filter by Brand (iHeartJane)",
        filter_options["brands"],
        index=filter_options["brands"].index(st.session_state.get("selected_brand", "ALL")),
        key="selected_brand"
    )
    selected_store = st.selectbox(
        "Filter by Store (iHeartJane)",
        filter_options["dispensaries"],
        index=filter_options["dispensaries"].index(st.session_state.get("selected_store", "ALL")),
        key="selected_store"
    )
    selected_type = st.selectbox(
        "Filter by Type (iHeartJane)",
        filter_options["categories"],
        index=filter_options["categories"].index(st.session_state.get("selected_type", "ALL")),
        key="selected_type"
    )
    search_term = st.text_input(
        "Search by Product Name (iHeartJane)",
        value=st.session_state.get("search_iheartjane", ""),
        key="search_iheartjane"
    )

    # Apply filters
    if selected_brand != "ALL":
        df_pivoted = df_pivoted[df_pivoted['brand'] == selected_brand]
    if selected_store != "ALL":
        df_pivoted = df_pivoted[df_pivoted['store_name'] == selected_store]
    if selected_type != "ALL":
        df_pivoted = df_pivoted[df_pivoted['type'] == selected_type]
    if search_term:
        df_pivoted = df_pivoted[df_pivoted['product_name'].str.contains(search_term, case=False)]

    # Display data
    st.title("iHeartJane Inventory and Sales Data")
    columns_to_display = ['store_name', 'product_name', 'type', 'price', 'brand'] + recent_dates
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

    with col2:
        st.header("Long Term Performance")
        top_sold_30_days = df_pivoted[['product_name', 'Sales_Last_30_Days']].sort_values('Sales_Last_30_Days', ascending=False)
        st.subheader("Last 30 Days")
        st.dataframe(top_sold_30_days)

        # Pie chart for top products by sales
        top_brands = df_pivoted.groupby('product_name')['Sales_Since_Yesterday'].sum().astype(float).nlargest(5)
        fig = px.pie(values=top_brands.values, names=top_brands.index, title='Top 5 Products by Sales Since Yesterday')
        st.plotly_chart(fig, use_container_width=True)
