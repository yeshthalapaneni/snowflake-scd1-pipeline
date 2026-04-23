# Snowflake SCD Type 1 Pipeline

This project demonstrates how to build a complete and automated data pipeline using Snowflake native features to process customer data with SCD Type 1 logic. The pipeline ingests both full-load and change data (CDC) files from Amazon S3 and updates a target customer table by always maintaining the latest values.

The design focuses on simplicity, automation, and using Snowflake capabilities instead of external orchestration tools.

---

## Overview

This pipeline handles two types of data inputs:

* Full load files (initial dataset)
* Change data capture (CDC) files (incremental updates)

A Python process generates these files and uploads them to Amazon S3. Snowflake then automatically ingests, tracks, and processes changes using Snowpipe, Streams, Tasks, and Stored Procedures.

The final output is a clean customer table that always reflects the most recent data.

---

## What is SCD Type 1

Slowly Changing Dimensions (SCD) are used to manage changes in dimension tables such as customers, products, or employees.

SCD Type 1 follows a simple rule:

* When data changes, overwrite the existing value
* Do not store history

Example:

| customer_id | name     | email                                       |
| ----------- | -------- | ------------------------------------------- |
| 1           | John Doe | [john@example.com](mailto:john@example.com) |

If the email changes to `john_new@example.com`, the table becomes:

| customer_id | name     | email                                               |
| ----------- | -------- | --------------------------------------------------- |
| 1           | John Doe | [john_new@example.com](mailto:john_new@example.com) |

The previous email is lost.

This approach is useful when only the latest version of data is needed.

---

## Architecture and Flow

The pipeline is built using a sequence of Snowflake-native components.

### Step 1: File Generation

A Python script generates:

* Full-load file (complete dataset)
* CDC file (new or updated records)

These files simulate incoming source data.

### Step 2: Storage in S3

Files are uploaded to Amazon S3. This acts as the raw data landing layer.

S3 provides:

* durable storage
* separation between ingestion and processing
* ability to replay or backfill data

### Step 3: Snowpipe Ingestion

Snowpipe continuously monitors the S3 location and automatically loads new files into a Snowflake landing table.

This removes the need for manual ingestion or scheduling.

### Step 4: Stream (Change Tracking)

A Snowflake Stream is created on the landing table.

The stream captures:

* new rows
* updated rows

This allows downstream logic to process only new changes instead of scanning the full dataset.

### Step 5: Stored Procedure (SCD Logic)

A stored procedure reads from the stream and applies SCD Type 1 logic.

The procedure performs a merge operation:

* if the record exists → update it
* if the record is new → insert it

This ensures the target table always has the latest values.

### Step 6: Task (Automation)

A Snowflake Task is used to run the stored procedure automatically.

The task can be configured to:

* run on a schedule
* or trigger when new data arrives

This makes the pipeline fully automated.

---

## Tech Stack

* Python (data generation)
* Amazon S3 (raw storage)
* Snowflake (data warehouse)
* Snowpipe (automated ingestion)
* Streams (change tracking)
* Tasks (scheduling)
* Stored Procedures (business logic)
* SQL (transformations)

---

## Project Structure

```text
snowflake-scd1-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── python/
│   └── generate_customer_files.py
├── sql/
│   ├── 01_create_stage.sql
│   ├── 02_create_pipe.sql
│   ├── 03_create_stream.sql
│   ├── 04_create_procedure.sql
│   ├── 05_create_task.sql
│   └── 06_run_validation.sql
└── sample_data/
    ├── customer_full_load.csv
    └── customer_cdc.csv
```

---

## Key Components Explained

### Snowpipe

* Automatically loads files from S3 into Snowflake
* Eliminates manual ingestion
* Works continuously as new files arrive

### Stream

* Tracks changes in a table
* Allows incremental processing
* Avoids reprocessing full datasets

### Stored Procedure

* Encapsulates business logic
* Implements SCD Type 1 merge
* Keeps logic reusable and maintainable

### Task

* Automates execution of SQL logic
* Removes need for external schedulers

---

## Data Flow Summary

1. Data files are generated
2. Files are uploaded to S3
3. Snowpipe loads data into Snowflake landing table
4. Stream captures new records
5. Stored procedure processes changes
6. Task automates execution
7. Target table is updated with latest values

---

## How to Run

### Step 1: Generate Data

Run the Python script to create sample files.

### Step 2: Upload to S3

Upload generated files to the configured S3 path.

### Step 3: Setup Snowflake

Run SQL scripts in order:

1. create stage
2. create pipe
3. create stream
4. create procedure
5. create task

### Step 4: Validate

Check the target table to confirm:

* new records are inserted
* existing records are updated

---

## Design Decisions

* S3 is used as a raw landing layer to decouple ingestion from processing
* Snowpipe is used to remove ingestion overhead
* Streams are used to enable incremental processing
* Stored procedures centralize business logic
* Tasks remove dependency on external orchestration tools

---

## When to Use This Approach

This design is suitable when:

* only the latest state of data is required
* history tracking is not needed
* data arrives in files
* a fully automated Snowflake-native pipeline is preferred

---

## Limitations

* No historical tracking of changes
* Overwrites previous values permanently

If history is required, SCD Type 2 would be more appropriate.

---

## Summary

This project demonstrates a practical implementation of SCD Type 1 using Snowflake native services. It shows how to build a simple, automated, and maintainable pipeline for keeping dimensional data up to date without external orchestration.
