'''
This script is for maintain a replication table of the Monthly NPI registry publication

Script Strategy: Direct Load into snowflake
Downloads ZIP → extracts CSV → saves locally → PUT to Snowflake stage → COPY INTO table

- Logging configured, drops a log file after every run.
- Extracting from the NPI website, file is dropped mid month
- Using requests.get we fetch the HTML of the website
- Using the HTML we parse it using Beautiful Soup
- The download link is present in an anchor tag
- We create a buffer and download it

# Snowflake cursor steps

- Start a transaction
- Truncate the table
- Create a temporary stage
- Upload the file to the Snowflake stage
- Load the data into the table
- Commit the transaction if everything is successful

'''

import requests
from bs4 import BeautifulSoup
import snowflake.connector
import re
from datetime import datetime
import zipfile
import io
import os
import logging
import time
from dotenv import load_dotenv

load_dotenv()

# Logging configurations
log_filename = f"LOG_{datetime.now().strftime('%Y_%B_%d')}.log"
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_filename),
                        logging.StreamHandler()
                    ])

logging.info(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

WEBSITE_URL = "https://download.cms.gov/nppes/NPI_Files.html"

start_time = time.time()

try:
    response = requests.get(WEBSITE_URL, timeout=(10, 1000))
    if response.status_code != 200:
        raise Exception(f"Failed to load page {WEBSITE_URL}")
    logging.info("Loaded Page")

    soup = BeautifulSoup(response.content, 'html.parser')

    # finding the anchor tag with an ID that starts with 'DDSMTH.ZIP'
    anchor_tag = soup.find('a', id=re.compile(r'^DDSMTH\.ZIP'))
    if not anchor_tag:
        raise Exception("Download link not found")

    # splits the url, in this case it will remove the NPI_Files.html part from
    # the website url, and concats the name of the file instead
    download_url = WEBSITE_URL.rsplit('/', 1)[0] + '/' + anchor_tag['href']

    # checking if download link is valid
    file_response = requests.get(download_url, stream=True, timeout=(10, 1000))
    if file_response.status_code != 200:
        raise Exception(f"Failed to download file from {download_url}")
    logging.info("Got 200 file_response")
    logging.info("Downloading...")

    # Save the downloaded file to a buffer
    buffer = io.BytesIO(file_response.content)
    desired_file_name = None
    desired_file_content = None

    logging.info("Unzipping file...")
    with zipfile.ZipFile(buffer, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.filename.startswith('endpoint_') and not file_info.filename.endswith('fileheader.csv'):
                desired_file_name = file_info.filename
                desired_file_content = zip_ref.read(file_info.filename)
                break
    
    logging.info("Unzipped")
    if not desired_file_name:
        raise Exception("Desired CSV file not found in the zip archive")
    logging.info("File Found")

    logging.info("Saving file...")
    # saving the desired file to the current working directory
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, desired_file_name)
    with open(file_path, 'wb') as file:
        file.write(desired_file_content)

    logging.info("Connecting to snowflake...")
    # Initialize the Snowflake connection
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    logging.info("Running Transaction...")
    try:
        cursor = conn.cursor()

        cursor.execute("BEGIN")

        cursor.execute("TRUNCATE TABLE PLAYGROUND_TEST.STAGE.ENDPOINT")

        cursor.execute("CREATE OR REPLACE TEMPORARY STAGE temp_stage")

        cursor.execute(f"PUT file://{file_path} @temp_stage")

        cursor.execute("""
            COPY INTO PLAYGROUND_TEST.STAGE.ENDPOINT
            FROM @temp_stage
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        """)

        cursor.execute("COMMIT")

        logging.info("Data loaded into Snowflake table successfully!")

    except Exception as e:
        # cancel and rollback the transaction in case of any error
        cursor.execute("ROLLBACK")
        error_message = f"An error occurred while loading data into Snowflake: {e}"
        logging.info("LOAD FAILED:")
        logging.error(error_message)
    finally:
        cursor.close()
        conn.close()

    os.remove(file_path)

except Exception as e:
    error_message = str(e)
    logging.error(error_message)

end_time = time.time()
execution_time = end_time - start_time
logging.info(f"Script executed in {execution_time:.2f} seconds")