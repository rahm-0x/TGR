import streamlit as st
import pandas as pd
import plotly.express as px
from filters import update_filter_options_dutchie

def calculate_sales(row, date_cols):
    reversed_date_cols = date_cols[::-1]
    diffs = row[reversed_date_cols].diff(-1).fillna(0).infer_objects()
    diffs = diffs[diffs >= 0]
    return diffs.sum()

def process_dutchie_data():
    if 'df_dutchie' in st.session_state:
        df_dutchie = st.session_state['df_dutchie']
        recent_dates = sorted([col for col in df_dutchie.columns if '-' in col], reverse=True)

        df_dutchie[recent_dates] = df_dutchie[recent_dates].fillna(0)

        if len(recent_dates) >= 2:
            df_dutchie['Sales_Since_Yesterday'] = df_dutchie[recent_dates[1]] - df_dutchie[recent_dates[0]]
        else:
            df_dutchie['Sales_Since_Yesterday'] = "NA"

        df_dutchie['Sales_Since_Yesterday'] = pd.to_numeric(df_dutchie['Sales_Since_Yesterday'], errors='coerce')

        last_3_days = recent_dates[:3]
        last_7_days = recent_dates[:7]
        last_30_days = recent_dates[:30]

        df_dutchie['Sales_Last_3_Days'] = df_dutchie.apply(lambda row: calculate_sales(row, last_3_days), axis=1)
        df_dutchie['Sales_Last_7_Days'] = df_dutchie.apply(lambda row: calculate_sales(row, last_7_days), axis=1)
        df_dutchie['Sales_Last_30_Days'] = df_dutchie.apply(lambda row: calculate_sales(row, last_30_days), axis=1)

        # Apply filters
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


        search_term_dutchie = st.text_input("Search (Dutchie)", "")
        if search_term_dutchie:
            df_dutchie = df_dutchie[df_dutchie.apply(lambda row: row.astype(str).str.contains(search_term_dutchie, case=False).any(), axis=1)]

        st.title("Dutchie Dispensary On-Hands & Sales Data")

        # Remove the "Sales_Last_3_Days", "Sales_Last_7_Days", and "Sales_Last_30_Days" columns from the main table
        columns_to_display = [col for col in df_dutchie.columns if col not in ['Sales_Last_3_Days', 'Sales_Last_7_Days', 'Sales_Last_30_Days']]
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
