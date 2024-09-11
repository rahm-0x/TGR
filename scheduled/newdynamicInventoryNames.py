import psycopg2
from sqlalchemy import create_engine
import toml
from datetime import datetime

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

        # Get the current date for the dynamic column
        snapshot_date = datetime.now().strftime('%Y_%m_%d')

        # Modify the database to add the new column dynamically for today if not exists
        cur.execute(f"""
            DO $$ 
            BEGIN 
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 't_dynamic_inventory_reverseddates' 
                    AND column_name = '{snapshot_date}'
                ) THEN
                    ALTER TABLE t_dynamic_inventory_reverseddates ADD COLUMN "{snapshot_date}" DOUBLE PRECISION;
                END IF;
            END $$;
        """)
        
        # Commit the changes
        conn.commit()

        # Now run the dynamic inventory function without arguments
        cur.execute("SELECT public.get_dynamic_inventory();")

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()

        print(f"Dynamic inventory function executed successfully, with snapshot column '{snapshot_date}' added.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run_dynamic_inventory_function()
