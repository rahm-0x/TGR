import psycopg2
from sqlalchemy import create_engine
import toml

def load_secrets(filepath):
    secrets = toml.load(filepath)
    db_config = {
        "user": secrets['user'],
        "password": secrets['password'],
        "host": secrets['host'],
        "port": secrets['port'],
        "dbname": secrets['dbname']
    }
    return db_config

def run_dynamic_inventory_function():
    try:
        # Load the .toml credentials
        db_config = load_secrets("/Users/phoenix/Desktop/TGR/secrets.toml")
        
        # Connection string for SQLAlchemy
        engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
        
        # Initialize DB connection
        conn = psycopg2.connect(
            host=db_config["host"],
            database=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"]
        )

        # Create a cursor object
        cur = conn.cursor()

        # Call the function
        cur.execute("SELECT public.get_dynamic_inventory();")

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()

        print("Dynamic inventory function executed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        # Optionally re-raise the exception to handle it further up in your stack
        # raise e

if __name__ == "__main__":
    run_dynamic_inventory_function()
