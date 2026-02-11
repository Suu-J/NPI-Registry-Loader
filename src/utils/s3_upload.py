'''
Website -> S3 Bucket

Here, it just uploads to S3 bucket after downloading the file
'''


import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime

# SOURCE URL
website_url = "https://download.cms.gov/nppes/NPI_Files.html"

s3_bucket_name = ""
s3_key_prefix = ""

aws_access_key_id = ''
aws_secret_access_key = ''
aws_region = ''

# Get HTML of the website
response = requests.get(website_url)
if response.status_code != 200:
    raise Exception(f"Failed to load page {website_url}")

# BSoup is a HTML parser!
soup = BeautifulSoup(response.content, 'html.parser')

# Lock on to the anchor tag with the known id
# This could be a potential fault as we do not know if id will remain constant
anchor_tag = soup.find('a', id="DDSMTH.ZIP.D240708")
if not anchor_tag:
    raise Exception("Download link not found")

# splits the url, in this case it will remove the NPI_Files.html part from
# the website url, and concats the name of the file instead
download_url = website_url.rsplit('/', 1)[0] + '/' + anchor_tag['href']

# checking if download link is valid
file_response = requests.get(download_url, stream=True)
if file_response.status_code != 200:
    raise Exception(f"Failed to download file from {download_url}")

# Making a dynamic s3 key using current month and year
current_datetime = datetime.now()
s3_file_key = f"{s3_key_prefix}NPPES_Data_Dissemination_{current_datetime.strftime('%B_%Y')}.zip"

# Initialize the S3 client with credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)

# Direct transfer of file from response stream to s3 location!!
try:
    print("Uploading...")
    s3_client.upload_fileobj(
        Fileobj=file_response.raw,
        Bucket=s3_bucket_name,
        Key=s3_file_key
    )
    print(f"File successfully uploaded to s3://{s3_bucket_name}/{s3_file_key}")
except NoCredentialsError:
    # This should not be required cause the credentials specified are correct.
    print("Credentials not available")
except Exception as e:
    print(f"An error occurred: {e}")