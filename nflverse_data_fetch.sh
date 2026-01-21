# Download
set -euo pipefail
BUCKET="gs://nflverse_data"
YEARS=("1999" "2000" "2001" "2002" "2003" "2004" "2005" "2006" "2007" "2008" "2009" "2010" "2011" "2012" "2013" "2014" "2015" "2016" "2017" "2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025")
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
RUN_ID="$(date +"%Y%m%d_%H%M%S")"
LOG_FILE="${LOG_DIR}/upload_${RUN_ID}.log"
echo "=======================================" | tee -a "$LOG_FILE"
echo "Download NFLverse data and upload to GCS" | tee -a "$LOG_FILE"
echo "Bucket: ${BUCKET}" | tee -a "$LOG_FILE"
echo "Log: ${LOG_FILE}" | tee -a "$LOG_FILE"
echo "=======================================" | tee -a "$LOG_FILE"
BASE_URL="https://github.com/nflverse/nflverse-data/releases/download/pbp"
for YEAR in "${YEARS[@]}"; do
    FILE="play_by_play_${YEAR}.parquet"
    URL="${BASE_URL}/${FILE}"
    DEST="${BUCKET}/${FILE}"
    echo "" | tee -a "$LOG_FILE"
    echo "[Downloading] ${URL}" | tee -a "$LOG_FILE"
    echo "[Uploading} ${DEST}" | tee -a "$LOG_FILE"
    curl -sS -L --retry 5 --retry-delay 2 --retry-all-errors "${URL}" | gsutil cp - "${DEST}" 2>&1 | tee -a "$LOG_FILE"
    echo "[OK] Uploaded ${FILE}" | tee -a "$LOG_FILE"
done
echo "" | tee -a "$LOG_FILE"
echo "Uploaded files:" | tee -a "$LOG_FILE"
gsutil ls -l "${BUCKET}/" | tee -a "$LOG_FILE"
echo "DONE." | tee -a "$LOG_FILE"

# Load to BigQuery
PROJECT_ID="my-673-project"
BQ_DATASET="NFLverse_dataset"
BQ_TABLE_PREFIX="play_by_play_"

echo "============================================================"
echo "BigQuery load (from existing GCS Parquet)"
echo "Project : ${PROJECT_ID}"
echo "Dataset : ${BQ_DATASET}"
echo "Bucket  : ${BUCKET}"
echo "Years   : ${YEARS[*]}"
echo "============================================================"

for Y in "${YEARS[@]}"; do
  OBJECT="${BUCKET}/play_by_play_${Y}.parquet"
  TABLE="${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE_PREFIX}${Y}"

  echo "----------------------------------------"
  echo "Year : ${Y}"
  echo "GCS  : ${OBJECT}"
  echo "BQ   : ${TABLE}"

  bq load \
    --location="us-west1" \
    --replace \
    --source_format=PARQUET \
    "${TABLE}" \
    "${OBJECT}"

  # Quick check
  bq head -n 3 "${TABLE}" || true
done

echo "Done loading to BigQuery"