# NPI to Snowflake Data Loader Scripts

Scripts for loading data into Snowflake, typically from downloaded files or S3.
Data comes from a gov website, once it's parsed, it's loaded into a snowflake table.

# currently pushing scripts
- direct_local_load_main.py -> renamed to NPI_direct_local_to_snowflake.py
- npi_failover_data_loader.py
- Phase2_npi_failover_data_loader.py

- Data_Load_Notif.py -> renamed to NPI_local_s3_SF.py

- s3_upload.py -> Needs renaming and comms audit

## ---------------------------------------------------------------------------------
## Primary Scripts:

## Direct Load Scripts
- **direct_local_load.py** - Downloads file, unzips, saves locally, loads to Snowflake
- **direct_local_load_main.py** - Main entry point for direct local loading
- **direct_stream_load.py** - Streams data directly into Snowflake without local save

## S3 Integration Scripts
- **Data_Load.py** - Downloads file, uploads to S3, calls Snowflake procedure
- **Data_Load_Notif.py** - Data loader with notification functionality
- **s3_upload.py** - Direct file upload to S3
- **unzip_s3_upload.py** - Unzips and uploads CSV to S3

## Autoloader Scripts
- **autoloader_rejected.py** - Autoloader with error handling and email notifications
- **main_copy_autoloader_main.py** - Main autoloader entry point

## Common Workflow
1. Download file from source URL
2. Unzip if necessary
3. Upload to S3 bucket
4. Call Snowflake stored procedure to load data
5. Send notification (optional)

## Dependencies
- snowflake-connector-python
- boto3 (for S3 operations)
- requests
- BeautifulSoup4
- zipfile

## Configuration Required
- Snowflake credentials
- AWS S3 credentials
- SMTP credentials (for notification scripts)
- Source URL for data files
