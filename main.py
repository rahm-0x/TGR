import subprocess
import os

# Paths to your scripts
DUTCHIE_SCRIPT = "/Users/phoenix/Desktop/TGR-Firebase/TGR/dataprocessing/dutchieZenFirebase.py"
IHEARTJANE_SCRIPT = "/Users/phoenix/Desktop/TGR-Firebase/TGR/dataprocessing/newIheatJane.py"
TYPESENSE_SCRIPT = "/Users/phoenix/Desktop/TGR-Firebase/TGR/dataprocessing/typesenseZenFirebase.py"
STANDARDIZE_SCRIPT = "/Users/phoenix/Desktop/TGR-Firebase/TGR/finalstandrized.py"
GOOGLE_SHEETS_SCRIPT = "/Users/phoenix/Desktop/TGR-Firebase/TGR/gdrive/finalgdrive.py"

def run_script(script_path):
    """
    Function to execute a Python script.
    """
    try:
        print(f"Running script: {os.path.basename(script_path)}...")
        result = subprocess.run(["python3", script_path], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Script {os.path.basename(script_path)} executed successfully.")
        else:
            print(f"Error executing {os.path.basename(script_path)}:\n{result.stderr}")
    except Exception as e:
        print(f"An error occurred while running {os.path.basename(script_path)}: {e}")

def validate_google_sheets():
    """
    Optional function to validate data in Google Sheets after the pipeline runs.
    """
    print("Validating Google Sheets update...")
    # Here you can add custom logic to verify the correctness of data in the Google Sheet.
    # For example, using the Google Sheets API to check data consistency or logs.
    print("Validation completed. Please check Google Sheets for updated data.")

if __name__ == "__main__":
    print("Starting the data processing pipeline...")

    # Step 1: Run Dutchie script
    run_script(DUTCHIE_SCRIPT)

    # Step 2: Run iHeartJane script
    run_script(IHEARTJANE_SCRIPT)

    # Step 3: Run Typesense script
    run_script(TYPESENSE_SCRIPT)

    # Step 4: Run standardization script
    run_script(STANDARDIZE_SCRIPT)

    # Step 5: Update Google Sheets
    run_script(GOOGLE_SHEETS_SCRIPT)

    # Step 6: Validate Google Sheets update (Optional)
    validate_google_sheets()

    print("Data processing pipeline completed.")
