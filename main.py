import subprocess
import time
import logging
import sys
import random
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

# Configure logging to write to a file and to the console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("script.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def run_script(script_name):
    logging.info(f'Running {script_name}...')
    result = subprocess.run(['python', script_name], capture_output=True, text=True)
    logging.info(f'{script_name} stdout:\n{result.stdout}')
    logging.info(f'{script_name} stderr:\n{result.stderr}')

def main():
    scripts_to_run_concurrently = [
        'dutchie.py',
        'iHeartJane.py',
        'weedmapsDataPull.py',
        'typesense.py',
        'curaleaf.py'
    ]

    # Run scripts concurrently
    with ProcessPoolExecutor() as executor:
        executor.map(run_script, scripts_to_run_concurrently)

    # Run follow-up scripts
    run_script('cleanNames.py')
    #run_script('cleanStrainNames.py')
    run_script('scheduled/dynamicInventoryNames.py')
    run_script('googleSheetsInventory.py')
    run_script('streamlit_app.py')

if __name__ == '__main__':
    while True:
        # Get current time and time for the next midnight
        now = datetime.now()
        next_midnight = datetime(now.year, now.month, now.day) + timedelta(days=1)
        
        # Calculate random sleep time between midnight and 2am
        random_seconds = random.randint(0, 7200)  # 0 to 7200 seconds (2 hours)
        
        # Calculate the time to wake up and run the script
        wakeup_time = next_midnight + timedelta(seconds=random_seconds) 
        
        # Calculate sleep time in seconds
        sleep_seconds = (wakeup_time - now).total_seconds()
        
        logging.info(f'Sleeping until {wakeup_time}...')
        time.sleep(sleep_seconds)

        main()
        logging.info('Completed all tasks for today.')
