"""
BUS 673 — End-to-end pipeline in Python (VS Code)

Goal:
1) Download CSV from the Dropbox URL (into memory)
2) Upload the CSV to Google Cloud Storage (GCS)
3) Load the CSV from GCS into a BigQuery table (autodetect schema, skip header, overwrite table)
4) Query a sample into a pandas DataFrame
Run a FULL-TABLE aggregation query in BigQuery:
        - group by fiscal year (fyear)
        - compute SUM(sale) and AVG(sale)
        (BigQuery scans full table, but the result is small: one row per year)
5) Build an interactive Plotly chart and save it to a local HTML file
6) Automatically open the HTML in your default browser


Why this design:
    # Streaming download + streaming upload is scalable: avoids memory/disk issues.
    # BigQuery load job is reproducible and works well for structured analytics.
    # Querying only a sample avoids pulling huge tables onto laptops.
    # Plotly HTML is portable: you can submit/view locally with no special setup.

Prereqs (one-time on your machine):
    # Install Google Cloud SDK and authenticate:
    gcloud auth application-default login
    gcloud config set project bus673

    # Install Python packages:
    python -m pip install google-cloud-storage google-cloud-bigquery pandas requests db-dtypes plotly
  (Optional but recommended for faster .to_dataframe()):
    python -m pip install google-cloud-bigquery-storage

    
 # LOGGING (add the syntax below to a script file)

        LOG_DIR="${LOG_DIR:-logs}"
        mkdir -p "$LOG_DIR"
        RUN_ID="$(date +"%Y%m%d_%H%M%S")"
        LOG_FILE="${LOG_DIR}/compustat_pipeline_${RUN_ID}.log"

        echo "Log file: ${LOG_FILE}"

    # Run python and tee all output to log
        python bus673_ide_end_to_end.py 2>&1 | tee -a "$LOG_FILE"   


Run:
    chmod +x run_compustat_pipeline.sh
    ./run_compustat_pipeline.sh

"""

import os
import webbrowser
import requests # type: ignore
import pandas as pd # type: ignore
import plotly.express as px # type: ignore

from google.cloud import storage
from google.cloud import bigquery


# =========================================================
# SETUP 
# =========================================================

# ============================================================
# Replace following  all variables with yours
  # PROJECT_ID
  # BUCKET_NAME
  # BQ_LOCATION 
  # DATASET_ID 
  # GCS_OBJECT_NAME 
  # TABLE_ID
  # DROPBOX_URL
  # OUTPUT_HTML
# ============================================================

# Your GCP project ID (must match what you use in Cloud Console)
PROJECT_ID = "my-673-project"

# GCS bucket settings
# IMPORTANT: This is the bucket NAME only (NO "gs://").
BUCKET_NAME = "charlie_the_bucket"

# The object (file) name inside the bucket.
# We keep .csv as the file extension since it is a CSV file.
GCS_OBJECT_NAME = "annual_data2000_2025.csv"

# BigQuery destination settings
# Keep dataset location consistent with how you created your dataset (often US for class)
BQ_LOCATION = "us-west1"

# Dataset that will contain the table (e.g., bus673_compustat)
DATASET_ID = "bus673_compustat"

# Table name in BigQuery.
# NOTE: BigQuery table IDs cannot include ".csv" so we use a clean table ID.
TABLE_ID = "annual_data2000_2025"

# Public data URL (Dropbox "dl=1" direct download link)
DROPBOX_URL = (
    "https://www.dropbox.com/scl/fi/7wfflagvvmqlyoifwrsch/annual_data2000_2025.csv"
    "?rlkey=pu0sgcfqh2h1py0a3ixskbky3&dl=1"
)

# Output HTML file created locally by Plotly
OUTPUT_HTML = "annual_sale_plot.html"

# When reading data back into pandas, NEVER pull the entire BigQuery table unless it is handlable, otherwise add query_limit
# QUERY_LIMIT = 200_000  

# Streaming buffer size (8MB is a good default); prevents memory problems when not saving file locally
CHUNK_SIZE = 8 * 1024 * 1024

# =========================================================
# 1) Stream download from URL -> stream upload to GCS
# =========================================================

# prints a progress message 
print("Step 1/6: Streaming download from Dropbox -> streaming upload to GCS...")

# create 
storage_client = storage.Client(project=PROJECT_ID)

# Get the bucket object (a pointer)
bucket = storage_client.bucket(BUCKET_NAME)

# create a blob - the file in the bucket 
# in GCS, files are called objects, and the Python client uses the term Blob
blob = bucket.blob(GCS_OBJECT_NAME)

# tells GCS that this file is CSV
blob.content_type = "text/csv"  

# requests.get(..., stream=True) means we read the response body incrementally
# Without stream=True: requests downloads the entire file into memory
# With stream=True: requests does NOT download everything immediately
# timeout is the maximum wait time for the sever to respond (in seconds)

with requests.get(DROPBOX_URL, stream=True, allow_redirects=True, timeout=300) as r:
    r.raise_for_status()

    # blob.open("wb") opens a writable stream to the object in GCS
    with blob.open("wb", chunk_size=CHUNK_SIZE) as gcs_f:
        for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:  # ignore keep-alive chunks
                gcs_f.write(chunk)

print(f"  ✓ Uploaded to gs://{BUCKET_NAME}/{GCS_OBJECT_NAME}")


# =========================================================
# 2) Load from GCS to BigQuery
# =========================================================
print("Step 2/6: Loading into BigQuery...")

bq_client = bigquery.Client(project=PROJECT_ID)

# Ensure dataset exists (create if missing)
dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
dataset.location = BQ_LOCATION
bq_client.create_dataset(dataset, exists_ok=True)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
gcs_uri = f"gs://{BUCKET_NAME}/{GCS_OBJECT_NAME}"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # header row
    autodetect=True,      # infer schema
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite each run
)

load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
load_job.result()  # wait until finished
print(f"  ✓ Load complete: {table_ref}")


# =========================================================
# STEP 3) Run FULL-TABLE aggregation query (small result)
# =========================================================
print("Step 3/6: Running aggregation query in BigQuery (full table scan, small output)...")

# IMPORTANT:
# - This query scans the full table, but returns only one row per year.
# - SAFE_CAST protects you if autodetect loaded columns as STRING.
query = f"""
SELECT
  SAFE_CAST(fyear AS INT64) AS fyear,
  SUM(SAFE_CAST(sale AS FLOAT64)) AS total_sales,
  AVG(SAFE_CAST(sale AS FLOAT64)) AS avg_sales
FROM `{table_ref}`
GROUP BY fyear
ORDER BY fyear;
"""

df = bq_client.query(query).to_dataframe()

print("\n===== Data Preview (df.head) =====")
print(df.head())


# =========================================================
# STEP 4) Plotly line chart (avg sales by fiscal year)
# =========================================================
print("Step 4/6: Building Plotly chart...")

fig = px.line(
    df,
    x="fyear",
    y="avg_sales",
    markers=True,
    title="Average Sales by Fiscal Year (Interactive)"
)
fig.update_layout(xaxis_title="Fiscal Year", yaxis_title="Average Sales")


# =========================================================
# STEP 5) Save Plotly chart to local HTML
# =========================================================
print("Step 5/6: Saving chart to HTML...")

fig.write_html(OUTPUT_HTML, auto_open=False)
abs_path = os.path.abspath(OUTPUT_HTML)
print(f"  ✓ Saved interactive webpage: {abs_path}")


# =========================================================
# STEP 6) Open in browser
# =========================================================
print("Step 6/6: Opening HTML in your browser...")

webbrowser.open(f"file://{abs_path}")
print("DONE.")