'''
PHASE 2 VERSION:
    
    CHECKS CONNECTION FIRST for saving time in case snowflake connection is at fault
TEST VERSION Uploads to TEST table:
    DOWNLOADS TO LOCAL AND UPLOADS TO SNOWFLAKE USING PROCEDURE WITH TRANSACTION.
    MATCH THE TABLE AND CSV BEING UPLOADED AT ALL TIMES
    FULL ABORT ON LOADERROR

    To select different file and table, change values in:
        extract_file
        truncate command
        copy into command
        select counts commands
'''

import io
from datetime import datetime
import re
import zipfile
import os
import logging
import time
import requests
import snowflake.connector
from bs4 import BeautifulSoup
from dotenv import load_dotenv

def configure_logging():
    log_filename = f"NPI_Loader_{datetime.now().strftime('%B_%Y_%d')}.log"
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler(log_filename),
                            logging.StreamHandler()
                        ])
    logging.info(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def load_env_variables():
    load_dotenv()
    required_vars = ['SNOWFLAKE_USER',
                     'SNOWFLAKE_PASSWORD',
                     'SNOWFLAKE_ACCOUNT',
                     'SNOWFLAKE_DATABASE',
                     'SNOWFLAKE_SCHEMA']
    for var in required_vars:
        if not os.getenv(var):
            raise EnvironmentError(f"Environment variable {var} not set")

def fetch_html(url):
    response = requests.get(url, timeout=(10, 1000))
    response.raise_for_status()
    logging.info("Loaded NPI Download Page")
    return response.content

def find_download_url(soup, base_url):
    anchor_tag = soup.find('a', id=re.compile(r'^DDSMTH\.ZIP'))
    if not anchor_tag:
        raise Exception("ZIP File Download link not found")
    return base_url.rsplit('/', 1)[0] + '/' + anchor_tag['href']

def download_file(url):
    response = requests.get(url, stream=True, timeout=(10, 1000))
    response.raise_for_status()
    logging.info("ZIP File Found")
    logging.info("Downloading ZIP File...")
    return io.BytesIO(response.content)

def extract_file(buffer):
    with zipfile.ZipFile(buffer, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.startswith('npi') and not file_info.filename.endswith('fileheader.csv'):
                logging.info("DATA File Found")
                return file_info.filename, zip_ref.read(file_info.filename)
    raise Exception("Desired DATA file not found in the zip")

def save_file(file_name, content):
    file_path = os.path.join(os.getcwd(), file_name)
    with open(file_path, 'wb') as file:
        file.write(content)
    logging.info("DATA File saved")
    return file_path

def connect_to_snowflake():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

def load_data_to_snowflake(file_path, conn):
    try:
        with conn.cursor() as cursor:
            with open(file_path, 'r') as file:
                csv_row_count = sum(1 for row in file) - 1  # Subtract 1 for the header row
            logging.info(f"Row count in CSV file (excluding header): {csv_row_count}")            

            cursor.execute("SELECT COUNT(*) FROM PLAYGROUND_TEST.STAGE.test_npi_data")
            initial_row_count = cursor.fetchone()[0]
            logging.info(f"Initial row count: {initial_row_count}")

            cursor.execute("BEGIN")
            cursor.execute("TRUNCATE TABLE PLAYGROUND_TEST.STAGE.test_npi_data")
            cursor.execute("CREATE OR REPLACE TEMPORARY STAGE temp_stage")
            cursor.execute(f"PUT file://{file_path} @temp_stage")
            cursor.execute("""
                COPY INTO PLAYGROUND_TEST.STAGE.test_npi_data
                FROM @temp_stage
                FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            """)

            cursor.execute("COMMIT")
            logging.info("Data loaded into Snowflake table successfully!")

            cursor.execute("SELECT COUNT(*) FROM PLAYGROUND_TEST.STAGE.test_npi_data")
            final_row_count = cursor.fetchone()[0]
            logging.info(f"Final row count: {final_row_count}")

            new_rows_added = final_row_count - initial_row_count
            logging.info(f"New rows added: {new_rows_added}")

    except Exception as e:
        conn.cursor().execute("ROLLBACK")
        logging.error(f"An error occurred while loading data into Snowflake: {e}")
        raise

def main():
    configure_logging()
    start_time = time.perf_counter()

    try:
        load_env_variables()
        conn = connect_to_snowflake()  # Establish Snowflake connection first
        html_content = fetch_html(WEBSITE_URL)
        soup = BeautifulSoup(html_content, 'html.parser')
        download_url = find_download_url(soup, WEBSITE_URL)
        buffer = download_file(download_url)
        file_name, file_content = extract_file(buffer)
        file_path = save_file(file_name, file_content)
        load_data_to_snowflake(file_path, conn)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)
            logging.info("DATA File removed after processing")
        if 'conn' in locals():
            conn.close()

    end_time = time.perf_counter()
    execution_time = end_time - start_time
    logging.info(f"Script executed in {execution_time:.2f} seconds")

if __name__ == "__main__":
    WEBSITE_URL = "https://download.cms.gov/nppes/NPI_Files.html"
    main()