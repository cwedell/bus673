import os
import webbrowser
import requests # type: ignore
import pandas as pd # type: ignore
import plotly.express as px # type: ignore

from google.cloud import storage
from google.cloud import bigquery

PROJECT_ID="my-673-project"
DATASET_ID="bus673_compustat"
TABLE_ID="annual_data2000_2025"

bq_client = bigquery.Client(project=PROJECT_ID)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

query = f"""
SELECT
  `at` AS firm_size,
  ((dltt + dlc)/`at`) AS leverage,
  (oibdp/sale) AS profitability,
  (xrd/sale) AS rnd_intensity,
  (emp/`at`) AS labor_intensity,
  (cogs/sale) AS cost_ratio,
  conm AS firm,
  fyear AS fiscal_year
FROM `{table_ref}`
WHERE sale <> 0 AND `at` <> 0
ORDER BY fyear ASC;
"""

df = bq_client.query(query).to_dataframe()

print("\n===== Data Preview (df.head) =====")
print(df.head())