from google.cloud import bigquery

PROJECT_ID="my-673-project"
DATASET_ID="bus673_compustat"
TABLE_ID="annual_data2000_2025"

bq_client = bigquery.Client(project=PROJECT_ID)

table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# query = f"""
# SELECT
#   `at` AS firm_size,
#   ((dltt + dlc)/`at`) AS leverage,
#   (oibdp/sale) AS profitability,
#   (xrd/sale) AS rnd_intensity,
#   (emp/`at`) AS labor_intensity,
#   (cogs/sale) AS cost_ratio,
#   conm AS firm,
#   fyear AS fiscal_year
# FROM `{table_ref}`
# WHERE sale <> 0 AND `at` <> 0
# ORDER BY fyear ASC;
# """

# df = bq_client.query(query).to_dataframe()

# print("\n===== Data Preview (df.head) =====")
# print(df.head())

query = f"""
SELECT
  LOG(`at`) AS LogAT,
  ((dltt + dlc)/`at`) AS leverage,
  (oibdp/sale) AS profitability,
  (xrd/sale) AS rnd_intensity,
  (emp/(`at`/1000000)) AS labor_intensity
FROM `{table_ref}`
WHERE sale <> 0 AND `at` <> 0;
"""

df = bq_client.query(query).to_dataframe()
df = df.dropna()

from sklearn.cluster import KMeans # type: ignore
import numpy as np # type: ignore
import matplotlib.pyplot as plt # type: ignore
from sklearn.preprocessing import StandardScaler # type: ignore

scaler = StandardScaler()
X_scaled = scaler.fit_transform(df)

kmeans = KMeans(n_clusters=4, random_state=42, init='k-means++', n_init=10)
kmeans.fit(X_scaled)
y_kmeans = kmeans.predict(X_scaled)

centers = kmeans.cluster_centers_
plt.scatter(df["LogAT"], df["leverage"], c=y_kmeans, s=50, cmap='viridis')
plt.scatter(centers[:, 0], centers[:, 1], c='black', s=200, alpha=0.5)
plt.xlabel("Logged Total Assets")
plt.ylabel("Leverage")
plt.title("Clusters")
plt.show()