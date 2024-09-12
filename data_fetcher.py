import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import hashlib

local_cache = {}

def hash_credentials(credentials):
    cred_str = ''.join(str(val) for val in credentials.values())
    return hashlib.md5(cred_str.encode()).hexdigest()

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
            return pd.DataFrame()  # Return empty DataFrame on error

    return local_cache[cache_key]
