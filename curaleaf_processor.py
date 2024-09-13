import streamlit as st
import pandas as pd
import plotly.express as px
from filters import update_filter_options_curaleaf

# Calculate sales for a range of dates
def calculate_sales(row, date_cols):
    reversed_date_cols = date_cols[::-1]
    diffs = row[reversed_date_cols].diff(-1).fillna(0).infer_objects()
    diffs = diffs[diffs >= 0]
    return diffs.sum()

def process_curaleaf_data():
    if 'df_curaleaf' in st.session_state:
        df_curaleaf = st.session_state['df_curaleaf']

        # Apply filters specific to Curaleaf data
        filter_options_curaleaf = update_filter_options_curaleaf(df_curaleaf)
        selected_dispensary_curaleaf = st.selectbox("Filter by Dispensary (Curaleaf)", filter_options_curaleaf["dispensaries"], key='dispensary_filter_curaleaf')
        if selected_dispensary_curaleaf != "ALL":
            df_curaleaf = df_curaleaf[df_curaleaf['dispensary_name'] == selected_dispensary_curaleaf]

        selected_category_curaleaf = st.selectbox("Filter by Category (Curaleaf)", filter_options_curaleaf["categories"], key='category_filter_curaleaf')
        if selected_category_curaleaf != "ALL":
            df_curaleaf = df_curaleaf[df_curaleaf['type'] == selected_category_curaleaf]

        selected_brand_curaleaf = st.selectbox("Filter by Brand (Curaleaf)", ["ALL"] + df_curaleaf['brand'].unique().tolist(), key='brand_filter_curaleaf')
        if selected_brand_curaleaf != "ALL":
            df_curaleaf = df_curaleaf[df_curaleaf['brand'] == selected_brand_curaleaf]

        search_term_curaleaf = st.text_input("Search (Curaleaf)", key='curaleaf_search')
        if search_term_curaleaf:
            df_curaleaf = df_curaleaf[df_curaleaf.apply(lambda row: row.astype(str).str.contains(search_term_curaleaf, case=False).any(), axis=1)]

        # Ensure correct date format for snapshot timestamps
        df_curaleaf['snapshot_timestamp'] = pd.to_datetime(df_curaleaf['snapshot_timestamp'])

        # Pivot data for snapshot columns (using timestamps as columns)
        df_pivoted = df_curaleaf.pivot_table(
            index=['dispensary_name', 'name', 'price', 'brand', 'type'],
            columns=pd.Grouper(key='snapshot_timestamp', freq='D'),  # Group by day to remove duplicates
            values='quantity',
            aggfunc='first'
        ).reset_index()

        # Display the pivoted data with correct column headers
        st.title("Curaleaf Inventory")
        st.dataframe(df_pivoted)

        # Dynamically find the most recent date columns (snapshot dates)
        recent_dates = sorted([col for col in df_pivoted.columns if isinstance(col, pd.Timestamp)], reverse=True)

        if len(recent_dates) >= 2:
            df_pivoted['Sales_Since_Yesterday'] = df_pivoted[recent_dates[1]] - df_pivoted[recent_dates[0]]
        else:
            df_pivoted['Sales_Since_Yesterday'] = "NA"

        df_pivoted['Sales_Since_Yesterday'] = pd.to_numeric(df_pivoted['Sales_Since_Yesterday'], errors='coerce')

        last_3_days = recent_dates[:3]
        last_7_days = recent_dates[:7]
        last_30_days = recent_dates[:30]

        df_pivoted['Sales_Last_3_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, last_3_days), axis=1)
        df_pivoted['Sales_Last_7_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, last_7_days), axis=1)
        df_pivoted['Sales_Last_30_Days'] = df_pivoted.apply(lambda row: calculate_sales(row, last_30_days), axis=1)

        # Filter for Out of Stock products (Available_Quantity == 0 or None)
        out_of_stock = df_pivoted[
            (df_pivoted[recent_dates[0]].isna()) | 
            (df_pivoted[recent_dates[0]] == 0)
        ][['dispensary_name', 'name', 'type', 'price', recent_dates[0]]]

        # Display sales data and pie chart
        col1, col2 = st.columns(2)

        with col1:
            st.header("Short Term Performance")
            top_sold_yesterday = df_pivoted[['name', 'Sales_Since_Yesterday']].sort_values('Sales_Since_Yesterday', ascending=False)
            top_sold_3_days = df_pivoted[['name', 'Sales_Last_3_Days']].sort_values('Sales_Last_3_Days', ascending=False)

            st.subheader("Yesterday")
            st.dataframe(top_sold_yesterday)
            st.subheader("Last 3 Days")
            st.dataframe(top_sold_3_days)
            st.header("Out of Stock Products")
            st.dataframe(out_of_stock.rename(columns={recent_dates[0]: 'Available Quantity'}))

        with col2:
            st.header("Long Term Performance")
            top_sold_7_days = df_pivoted[['name', 'Sales_Last_7_Days']].sort_values('Sales_Last_7_Days', ascending=False)
            top_sold_30_days = df_pivoted[['name', 'Sales_Last_30_Days']].sort_values('Sales_Last_30_Days', ascending=False)

            st.subheader("Last 7 Days")
            st.dataframe(top_sold_7_days)
            st.subheader("Last 30 Days")
            st.dataframe(top_sold_30_days)

            # Pie chart for top products by sales since yesterday
            top_brands = df_pivoted.groupby('name')['Sales_Since_Yesterday'].sum().nlargest(5)
            fig = px.pie(values=top_brands.values, names=top_brands.index, title='Top 5 Products by Sales Since Yesterday', labels={'value': 'Sales Since Yesterday'})
            fig.update_traces(textinfo='label+value')
            st.plotly_chart(fig, use_container_width=True)
