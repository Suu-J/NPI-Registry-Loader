'''
# Pending docstring
# remove commandlines
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

# Configure logging
log_filename = f"LOG_{datetime.now().strftime('%Y_%B_%d')}.log"
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_filename),
                        logging.StreamHandler()
                    ])
# Log the start date and time
logging.info(f"Script started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# SOURCE URL where the download file is located.
WEBSITE_URL = "https://download.cms.gov/nppes/NPI_Files.html"

start_time = time.time()

try:
    # Get HTML of the website
    response = requests.get(WEBSITE_URL, timeout=(10, 1000))
    if response.status_code != 200:
        raise Exception(f"Failed to load page {WEBSITE_URL}")
    logging.info("Loaded Page")

    # BSoup is a HTML parser!
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the anchor tag with an ID that starts with 'DDSMTH.ZIP'
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
    # Save the desired file to the current working directory
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

        # Start a transaction
        cursor.execute("BEGIN")

        # Truncate the table
        cursor.execute("TRUNCATE TABLE PLAYGROUND_TEST.STAGE.ENDPOINT")

        # Create a temporary stage
        cursor.execute("CREATE OR REPLACE TEMPORARY STAGE temp_stage")

        # Upload the file to the Snowflake stage
        cursor.execute(f"PUT file://{file_path} @temp_stage")

        # Load the data into the table
        cursor.execute("""
            COPY INTO PLAYGROUND_TEST.STAGE.ENDPOINT
            FROM @temp_stage
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        """)

        # Commit the transaction if everything is successful
        cursor.execute("COMMIT")

        logging.info("Data loaded into Snowflake table successfully!")

    except Exception as e:
        # Rollback the transaction in case of any error
        cursor.execute("ROLLBACK")
        error_message = f"An error occurred while loading data into Snowflake: {e}"
        logging.info("LOAD FAILED:")
        logging.error(error_message)
    finally:
        cursor.close()
        conn.close()

    # Clean up the file
    os.remove(file_path)

except Exception as e:
    error_message = str(e)
    logging.error(error_message)

end_time = time.time()
execution_time = end_time - start_time
logging.info(f"Script executed in {execution_time:.2f} seconds")