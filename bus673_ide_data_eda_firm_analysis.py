"""
Prereqs (run in terminal before this script):

    python -m venv venv
    source venv/bin/activate        # macOS/Linux
    # venv\\Scripts\\activate       # Windows

    pip3 install google-cloud-bigquery pandas numpy linearmodels matplotlib
    pip3 install google-cloud-bigquery-storage


    gcloud auth login
    gcloud config set project sharp-footing-478019-d7
    gcloud auth application-default login

    # run the script
    # cd to folder where you saved the .py file  
    python3 xxxxx.py
    
"""

# -------------------------
# IMPORT LIBRARY
# -------------------------
from google.cloud import bigquery
import pandas as pd # type: ignore
import matplotlib.pyplot as plt # type: ignore

# -------------------------
# CONFIGURATION
# -------------------------
# Update these if your project/dataset/table names differ.
PROJECT_ID = "my-673-project"                 # your GCP project id
DATASET = "bus673_compustat"                  # your BigQuery dataset
TABLE = "annual_data2000_2025"                # your Compustat annual table

FULL_TABLE_NAME = f"`{PROJECT_ID}.{DATASET}.{TABLE}`"


# -------------------------
# HELPER: BUILD CLIENT
# -------------------------
def get_bq_client() -> bigquery.Client:
    """Create and return a BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)


# -------------------------
# E2: distribution of firm sales growth (quantiles)
# -------------------------
def run_e2_growth_distribution(client: bigquery.Client) -> pd.DataFrame:
    """
    E 2: Compute year-over-year sales growth distribution
    and summarize with quantiles across firms by year.
    """

    query = f"""
    WITH base AS (
      SELECT
        gvkey,
        fyear,
        sale,
        LAG(sale) OVER (PARTITION BY gvkey ORDER BY fyear) AS sale_lag
      FROM {FULL_TABLE_NAME}
    ),

    growth AS (
      SELECT
        gvkey,
        fyear,
        SAFE_DIVIDE(sale - sale_lag, sale_lag) AS yoy_growth
      FROM base
      WHERE sale_lag IS NOT NULL
    )

    SELECT
      fyear,
      APPROX_QUANTILES(yoy_growth, 20)[OFFSET(1)]  AS p05,
      APPROX_QUANTILES(yoy_growth, 20)[OFFSET(5)]  AS p25,
      APPROX_QUANTILES(yoy_growth, 20)[OFFSET(10)] AS p50,
      APPROX_QUANTILES(yoy_growth, 20)[OFFSET(15)] AS p75,
      APPROX_QUANTILES(yoy_growth, 20)[OFFSET(19)] AS p95
    FROM growth
    GROUP BY fyear
    ORDER BY fyear
    """

    print("\n[E 2] Running growth distribution quantiles query...")
    df = client.query(query).to_dataframe()
    print("[E 2] Sample output:")
    print(df.head())

    # Visualization: plot multiple quantile lines over time
    print("[E 2] Creating visualization...")

    plt.figure(figsize=(10, 6))
    plt.plot(df["fyear"], df["p05"], marker="o", label="p05")
    plt.plot(df["fyear"], df["p25"], marker="o", label="p25")
    plt.plot(df["fyear"], df["p50"], marker="o", label="p50 (median)")
    plt.plot(df["fyear"], df["p75"], marker="o", label="p75")
    plt.plot(df["fyear"], df["p95"], marker="o", label="p95")
    plt.xlabel("Fiscal Year")
    plt.ylabel("YoY Sales Growth")
    plt.title("Distribution of Firm YoY Sales Growth (Quantiles over Time)")
    plt.legend()
    plt.grid(False)  # white background, no grid
    plt.tight_layout()

    plt.savefig("e2_firm_sale_growth.png", dpi=300, bbox_inches="tight")

    plt.show()

    return df

# -------------------------
# MAIN
# -------------------------
def main():
    client = get_bq_client()

    # Run each analytic task + visualization
    df_e2 = run_e2_growth_distribution(client)

    # Optionally, save outputs to CSVs for later use or grading
    df_e2.to_csv("e2_growth_distribution.csv", index=False)

    print("\nCSV files saved in current directory.")


if __name__ == "__main__":
    main()