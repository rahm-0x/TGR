import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import hashlib

# Local cache dictionary
local_cache = {}

# Function to hash credentials for caching
def hash_credentials(credentials):
    cred_str = ''.join(str(val) for val in credentials.values())
    return hashlib.md5(cred_str.encode()).hexdigest()

# Function to fetch inventory data with optional cache bypass
def fetch_inventory_data(query, bypass_cache=False):
    # Fetch credentials from secrets
    credentials = {
        'dbname': st.secrets["dbname"],
        'user': st.secrets["user"],
        'password': st.secrets["password"],
        'host': st.secrets["host"],
        'port': st.secrets["port"],
    }
    
    # Generate cache key based on credentials and query hash
    cred_hash = hash_credentials(credentials)
    cache_key = f"{cred_hash}:{hashlib.md5(query.encode()).hexdigest()}"

    # Check if data is already cached and bypass_cache is not set
    if cache_key not in local_cache or bypass_cache:
        st.write(f"Fetching data for query: {query}")  # Debugging output
        try:
            # Connect to the database using SQLAlchemy
            engine = create_engine(f"postgresql+psycopg2://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['dbname']}")
            with engine.connect() as connection:
                # Fetch the data
                df = pd.read_sql(query, connection)
                # Cache the result
                local_cache[cache_key] = df
                st.write(f"Data fetched successfully. Caching with key: {cache_key}")  # Debugging output
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error

    else:
        st.write(f"Using cached data for query: {query}")  # Debugging output

    # Return cached or freshly fetched data
    return local_cache[cache_key]

# Optionally clear the local cache (can be added as a button in the app)
def clear_local_cache():
    local_cache.clear()
    st.write("Local cache cleared!")
