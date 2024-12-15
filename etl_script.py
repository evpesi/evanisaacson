import requests
import csv
import time
import io
import boto3
from botocore.exceptions import NoCredentialsError

# AWS S3 configuration
S3_BUCKET = 'evan-data-project' 
S3_KEY = 'CMS_data/CMS_Unplanned_Hospital_Visits.csv'  # Path and filename in S3

# Initialize an S3 client (relies on preset credentials from environment)
s3_client = boto3.client('s3')

# Set up our connection to the healthcare database
database_url = 'https://data.cms.gov/provider-data/api/1/datastore/query/632h-zaca/0'
how_to_talk = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

# Prepare for data collection
records_per_request = 2000
starting_point = 0
all_unique_records = []
seen_records = set()

# Start collecting data
while True:
    # Ask for a batch of data
    request_details = {
        "limit": records_per_request,
        "offset": starting_point,
        "sort": [{"property": "record_number", "direction": "asc"}]
    }

    # Send request and get response
    response = requests.post(database_url, headers=how_to_talk, json=request_details)
    data_received = response.json()

    # Check if we got what we asked for
    if "results" not in data_received:
        print("Oops! The database didn't give us the results we expected. Let's stop here.")
        break

    batch_of_records = data_received.get("results", [])
    if not batch_of_records:
        print("Looks like we've collected all the data there is!")
        break

    # Find new, unique records
    new_records = []
    for record in batch_of_records:
        # Create a unique fingerprint to ensure there are no duplicates
        record_fingerprint = record.get('facility_id', '') + record.get('measure_id', '')
        if record_fingerprint not in seen_records:
            seen_records.add(record_fingerprint)
            new_records.append(record)

    all_unique_records.extend(new_records)

    # Give an update on our progress
    print(f"We just fetched {len(batch_of_records)} records, {len(new_records)} were new. "
          f"We now have {len(all_unique_records)} unique records in total.")

    # Prepare for the next batch
    starting_point += len(batch_of_records)

    # Stop if we didn't find any new records
    if len(new_records) == 0:
        print("We didn't find any new records in this batch. Done!")
        break

    # Take a short breather to be nice to the database
    time.sleep(0.5)

# If we have data, upload it directly to S3 in CSV format
if all_unique_records:
    # Determine columns from the first record
    column_names = all_unique_records[0].keys()

    # Write CSV data to an in-memory buffer
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=column_names)
    writer.writeheader()
    writer.writerows(all_unique_records)

    try:
        # Upload the CSV content from memory to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"Great news! All the data has been uploaded to s3://{S3_BUCKET}/{S3_KEY}")
    except NoCredentialsError:
        print("Error: AWS credentials not found. Please configure.")
else:
    print("We didn't find any data to save. Better luck next time!")
