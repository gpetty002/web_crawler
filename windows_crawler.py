from selenium import webdriver
from tenacity import retry, wait_fixed, stop_after_attempt, RetryError
import logging
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from selenium.webdriver.chrome.options import Options
from browsermobproxy import Server
import contextlib
import pandas as pd
import json
import os
import requests
from requests.adapters import HTTPAdapter

# Adjust connection pool size and retries
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry, pool_connections=500, pool_maxsize=500)
session.mount('http://', adapter)
session.mount('https://', adapter)

logging.basicConfig(filename="myLog.txt", level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")

server = Server("browsermob/bin/browsermob-proxy")
server.start()
proxy = server.create_proxy()
output_dir = "webCrawl/har_fil"
os.makedirs(output_dir, exist_ok=True)

@contextlib.contextmanager 
def create_driver(): 
    chrome_options = Options()
    chrome_options.add_argument(f"--proxy-server={proxy.proxy}")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    # Increase the timeout settings to 120 seconds
    driver.set_page_load_timeout(120)
    driver.implicitly_wait(120)
    driver.set_script_timeout(120)
    
    try: 
        yield driver
    finally: 
        driver.quit()

# Read CSV file
sites = pd.read_csv("webCrawl/top-1m.csv", header=None, names=["index", "url"])

@retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
def visit_and_save(url, index, total, driver):
    logging.warning(f"Visiting site {index + 1}/{total}: {url}")

    if not url.startswith(("http://", "https://")):
        url = "http://" + url

    proxy.new_har(url)
    try:
        driver.get(url)
    except Exception as e:
        logging.error(f"Error visiting {url}: {e}")
        return
    har_data = proxy.har

    har_filename = os.path.join(output_dir, f"{url.replace('.', '_').replace('/', '_')}.har")
    with open(har_filename, "w") as har_file:
        json.dump(har_data, har_file)
  
    logging.warning(f"Successfully site # {index + 1} saved HAR file for {url}")

totalSites = len(sites)

def process_site(site, i, total, driver):
    try:
        visit_and_save(site, i, total, driver)
    except Exception as e:
        logging.warning(f"Failed to visit and save #{i}: {site}, Error: {e}")

max_workers = 100
start_index = 0

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    with create_driver() as driver:
        futures = [executor.submit(process_site, site, i, totalSites, driver) for i, site in enumerate(sites['url'][start_index:], start=start_index)]
        for future in as_completed(futures):
            future.result()

server.stop()
