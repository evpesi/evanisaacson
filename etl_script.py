import requests
import csv
import time

api_url = 'https://data.cms.gov/provider-data/api/1/datastore/query/632h-zaca/0'
headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

limit = 2000
offset = 0  # Start at the first record
all_records = []  # Store all unique records
unique_record_ids = set()  # Track unique records

while True:
    # Prepare the API request payload with offset
    data = {
        "limit": limit,
        "offset": offset,
        "sort": [{"property": "record_number", "direction": "asc"}]  # Ensure consistent order
    }

    response = requests.post(api_url, headers=headers, json=data)
    response_json = response.json()

    # Check for 'results' key
    if "results" not in response_json:
        print("No 'results' key found in response. Exiting.")
        break

    records = response_json.get("results", [])
    if not records:
        print("No more data found.")
        break

    # Filter and append only unique records
    new_records = []
    for record in records:
        record_id = record.get('facility_id') + record.get('measure_id', '')  # Unique ID logic
        if record_id not in unique_record_ids:
            unique_record_ids.add(record_id)
            new_records.append(record)

    all_records.extend(new_records)

    # Debugging Output
    print(f"Fetched {len(records)} records, {len(new_records)} new. Total unique so far: {len(all_records)}")

    # Increment the offset for the next batch
    offset += len(records)

    # Stop the loop if no new records were added
    if len(new_records) == 0:
        print("No new records found in this batch. Exiting loop.")
        break

    # Optional: Add delay to prevent rate-limiting
    time.sleep(0.5)

# Write the collected data to a CSV file
csv_file = "cms_data.csv"
if all_records:
    # Extract field names from the first record
    fieldnames = all_records[0].keys()

    with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_records)

    print(f"Data successfully saved to {csv_file}")
else:
    print("No data to save.")
