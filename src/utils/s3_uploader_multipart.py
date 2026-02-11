import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime
import zipfile
import io
from tqdm import tqdm

website_url = "https://download.cms.gov/nppes/NPI_Files.html"

s3_bucket_name = ""
s3_key_prefix = ""

aws_access_key_id = ''
aws_secret_access_key = ''
aws_region = ''

response = requests.get(website_url)
if response.status_code != 200:
    raise Exception(f"Failed to load page {website_url}")
else:
    print("Loaded Page")

soup = BeautifulSoup(response.content, 'html.parser')

# Lock on to the anchor tag with the known id
# This could be a potential fault as we do not know if id will remain constant
anchor_tag = soup.find('a', id="DDSMTH.ZIP.D240708")
if not anchor_tag:
    raise Exception("Download link not found")

# splits the url, in this case it will remove the NPI_Files.html part from
# the website url, and concats the name of the file instead
download_url = website_url.rsplit('/', 1)[0] + '/' + anchor_tag['href']

file_response = requests.get(download_url, stream=True)
if file_response.status_code != 200:
    raise Exception(f"Failed to download file from {download_url}")
else:
    print("Got 200 file_response")

current_datetime = datetime.now()
s3_file_key = f"{s3_key_prefix}TESTING_GLUE.csv"


s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_region
)
print("Downloading...")
buffer = io.BytesIO(file_response.content)
desired_file_name = None
desired_file_content = None

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
except NoCredentialsError:
    print("Credentials not available")
except Exception as e:
    print(f"An error occurred: {e}")
