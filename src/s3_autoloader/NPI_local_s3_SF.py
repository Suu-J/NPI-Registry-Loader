'''
Process Flow:
Download File -> Upload to S3 -> Transfer to Snowflake

Email notification was integrated within the script,
Could have modularised this to be honest, but this was for quick prototyping

Snowflake Procedure Triggered within the script
ID hybridized using re

tqdm for uploading bar

'''

import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime
import zipfile
import io
from tqdm import tqdm
import snowflake.connector
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# SOURCE URL where the download file is located.
website_url = "https://download.cms.gov/nppes/NPI_Files.html"

#creds
s3_bucket_name = ""
s3_key_prefix = ""

aws_access_key_id = ''
aws_secret_access_key = ''
aws_region = ''

# Email configuration
smtp_server = 'outlook.office365.com'
smtp_port = 587
smtp_user = ''
smtp_password = ''
recipient_email = ''

def send_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = recipient_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        text = msg.as_string()
        server.sendmail(smtp_user, recipient_email, text)
        server.quit()
        print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {e}")


# Fetching HTML of the website
response = requests.get(website_url)
if response.status_code != 200:
    raise Exception(f"Failed to load page {website_url}")
else:
    print("Loaded Page")

# Parsing HTML using bsoup
soup = BeautifulSoup(response.content, 'html.parser')

# Lock on to the anchor tag with the known id
# This could be a potential fault as we do not know if id will remain constant
# anchor_tag = soup.find('a', id="DDSMTH.ZIP.D240708")
# if not anchor_tag:
#     raise Exception("Download link not found")

# Find the anchor tag with an ID that starts with 'DDSMTH.ZIP'
anchor_tag = soup.find('a', id=re.compile(r'^DDSMTH\.ZIP'))
if not anchor_tag:
    raise Exception("Download link not found")

# splits the url, in this case it will remove the NPI_Files.html part from
# the website url, and concats the name of the file instead
download_url = website_url.rsplit('/', 1)[0] + '/' + anchor_tag['href']

# checking if download link is valid
file_response = requests.get(download_url, stream=True)
if file_response.status_code != 200:
    raise Exception(f"Failed to download file from {download_url}")
else:
    print("Got 200 file_response")

# Making a dynamic s3 key using current month and year
# current_datetime = datetime.now()
# s3_file_key = f"{s3_key_prefix}NPPES_Data_Dissemination_{current_datetime.strftime('%B_%Y')}.csv"
s3_file_key = f"{s3_key_prefix}TESTING_GLUE.csv"

# Initialize the S3 client with credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)
print("Downloading...")
# Unzip the file in memory and find the desired CSV file
buffer = io.BytesIO(file_response.content)
desired_file_name = None
desired_file_content = None
print("Unzipping")
with zipfile.ZipFile(buffer, 'r') as zip_ref:
    for file_info in zip_ref.infolist():
        if file_info.filename.startswith('endpoint_') and not file_info.filename.endswith('fileheader.csv'):
            desired_file_name = file_info.filename
            desired_file_content = zip_ref.read(file_info.filename)
            break
print("Unzipped")
if not desired_file_name:
    raise Exception("Desired CSV file not found in the zip archive")
else:
    print("File Found")

# Upload the desired CSV file to S3 using multipart upload with progress tracking
try:
    print("Uploading the desired CSV file using multipart upload...")
    multipart_upload = s3_client.create_multipart_upload(Bucket=s3_bucket_name, Key=s3_file_key)
    part_size = 5 * 1024 * 1024  # 5 MB
    parts = []
    total_parts = (len(desired_file_content) + part_size - 1) // part_size

    for i in tqdm(range(0, len(desired_file_content), part_size), desc="Uploading parts", unit="part"):
        part_number = len(parts) + 1
        part = s3_client.upload_part(
            Bucket=s3_bucket_name,
            Key=s3_file_key,
            PartNumber=part_number,
            UploadId=multipart_upload['UploadId'],
            Body=desired_file_content[i:i + part_size]
        )
        parts.append({'PartNumber': part_number, 'ETag': part['ETag']})

    s3_client.complete_multipart_upload(
        Bucket=s3_bucket_name,
        Key=s3_file_key,
        UploadId=multipart_upload['UploadId'],
        MultipartUpload={'Parts': parts}
    )
    print(f"Desired CSV file successfully uploaded to s3://{s3_bucket_name}/{s3_file_key}")

    # Call the Snowflake stored procedure
    def call_snowflake_procedure():
        conn = snowflake.connector.connect(
            user='',
            password='',
            account='',
            database = '',
            schema = ''
        )
        try:
            cursor = conn.cursor()
            print("Calling Procedure")
            cursor.execute("CALL PLAYGROUND_TEST.STAGE.reload_data_from_s3();")
            result = cursor.fetchone()
            print(result[0])  # Print the result from the stored procedure
            send_email("Snowflake Data Reload Status", result[0])
        except Exception as e:
            error_message = f"An error occurred while calling the procedure: {e}"
            print(error_message)
            send_email("Snowflake Data Reload Status", error_message)
        finally:
            cursor.close()
            conn.close()

    call_snowflake_procedure()
    print("Done.")

except NoCredentialsError:
    error_message = "Credentials not available"
    print(error_message)
    send_email("Snowflake Data Reload Status", error_message)

except Exception as e:
    error_message = f"An error occurred: {e}"
    print(error_message)
    send_email("Snowflake Data Reload Status", error_message)

# No need to delete files locally as they are not stored on the local system