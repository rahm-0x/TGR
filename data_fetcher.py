import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Function to fetch inventory data from PostgreSQL database
def fetch_inventory_data(query):
    credentials = {
        'dbname': st.secrets["dbname"],
        'user': st.secrets["user"],
        'password': st.secrets["password"],
        'host': st.secrets["host"],
        'port': st.secrets["port"],
    }

    try:
        engine = create_engine(f"postgresql+psycopg2://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['dbname']}")
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

    return df
