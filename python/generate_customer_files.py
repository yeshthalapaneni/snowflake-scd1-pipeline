"""
Generate Customer Full Load and CDC Files

This script creates two CSV files:
1. Full load file (initial dataset)
2. CDC file (new and updated records)

These files simulate source system data and can be uploaded to S3
for ingestion into Snowflake.
"""

import csv
import os
from datetime import datetime

OUTPUT_DIR = "../sample_data"

# -----------------------------
# Sample Data
# -----------------------------

FULL_LOAD_DATA = [
    {"customer_id": 1, "name": "John Doe", "email": "john@example.com"},
    {"customer_id": 2, "name": "Jane Smith", "email": "jane@example.com"},
    {"customer_id": 3, "name": "Mike Brown", "email": "mike@example.com"},
]

CDC_DATA = [
    # Existing record update
    {"customer_id": 1, "name": "John Doe", "email": "john_new@example.com"},
    # New record insert
    {"customer_id": 4, "name": "Emily Davis", "email": "emily@example.com"},
]

# -----------------------------
# Helper Functions
# -----------------------------

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def write_csv(file_path, data):
    if not data:
        return

    fieldnames = data[0].keys()

    with open(file_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

# -----------------------------
# Main Execution
# -----------------------------

def main():
    ensure_output_dir()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    full_load_file = os.path.join(OUTPUT_DIR, f"customer_full_load_{timestamp}.csv")
    cdc_file = os.path.join(OUTPUT_DIR, f"customer_cdc_{timestamp}.csv")

    write_csv(full_load_file, FULL_LOAD_DATA)
    write_csv(cdc_file, CDC_DATA)

    print(f"Full load file created: {full_load_file}")
    print(f"CDC file created: {cdc_file}")


if __name__ == "__main__":
    main()
