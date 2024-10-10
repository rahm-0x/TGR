from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

def fetch_data_with_selenium():
    # Setup Selenium with ChromeDriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode (no browser UI)
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        # Open the iHeartJane page for the store
        driver.get('https://www.iheartjane.com/stores/888/cookies-on-the-strip/menu')
        time.sleep(5)  # Wait for the page to fully load

        # Example of finding an element on the page to verify
        page_source = driver.page_source
        print("Page Source Length:", len(page_source))

        # More logic can be added here to interact with the page
        # or extract specific information dynamically

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    fetch_data_with_selenium()
