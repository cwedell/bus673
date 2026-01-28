from playwright.sync_api import sync_playwright # type: ignore

def fetch_html(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)        
        context = browser.new_context(
            java_script_enabled=False,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )        
        page = context.new_page()
        page.set_default_timeout(60000) # 60 seconds        
        page.goto(url, wait_until="domcontentloaded")
        html = page.content()        
        browser.close()
        return html

import re
from html.parser import HTMLParser

class SimpleExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_p = False
        self.buffer = []
        self.paragraphs = []

    def handle_starttag(self, tag, attrs):
        if tag == "p":
            self.in_p = True
            self.buffer = []

    def handle_endtag(self, tag):
        if tag == "p" and self.in_p:
            text = "".join(self.buffer)
            cleaned = " ".join(text.split())
            self.paragraphs.append(cleaned)
            self.in_p = False

    def handle_data(self, data):
        if self.in_p:
            self.buffer.append(data)

record_re = re.compile(r"(\d+-\d+-\d+)")
coach_re = re.compile(r"Coach:\s*(.*)")

def extract_coaches(paragraph):
    m = coach_re.search(paragraph)
    if not m:
        return []

    text = m.group(1)
    text = re.sub(r"\(\d+-\d+-\d+\)", "", text) # Remove the (W-L-T) records
    parts = re.split(r"\s+and\s+|,\s*", text) # Split on "and" or commas
    names = [p.strip() for p in parts if p.strip()] # Clean whitespace

    return names

def extract_info_from_html(html):
    parser = SimpleExtractor()
    parser.feed(html)

    record = None
    coaches = []

    for p in parser.paragraphs:
        text = p.strip()

        if text.startswith("Record:"):
            m = record_re.search(text)
            if m:
                record = m.group(1)

        if text.startswith("Coach:"):
            coaches = extract_coaches(text)

    return record, coaches

teams = ["crd", "sea", "sfo", "ram",
         "gnb", "min", "det", "chi",
         "dal", "phi", "nyg", "was",
         "nor", "atl", "tam", "car",
         "kan", "den", "sdg", "rai",
         "pit", "rav", "cin", "cle",
         "nwe", "buf", "mia", "nyj",
         "clt", "oti", "htx", "jax"]
years = range(1999, 2026) # go to 2026, range excludes final year

import pandas as pd # type: ignore
import time
import random

def scrape_teams(teams):
    rows = []

    for team in teams:
        print(f"Scraping team: {team}")
        for year in years:
            if team == "htx" and year < 2002: # they didn't exist yet
                continue
            
            url = f"https://www.pro-football-reference.com/teams/{team}/{year}.htm"
            html = fetch_html(url)
            record, coaches = extract_info_from_html(html)

            rows.append({
                "team": team,
                "year": year,
                "record": record,
                "coaches": coaches
            })
            
            time.sleep(random.uniform(1.5, 3.5)) # be nice to them

    return pd.DataFrame(rows)

df = scrape_teams(teams)

# save file
local_csv = "NFL_scraped_PFR.csv"
df.to_csv(local_csv, index=False)

# load to GCP/BQ
GCP_PROJECT = "my-673-project"
GCS_BUCKET  = "charlie_the_bucket"
GCS_BLOB    = "NFL_scraped_PFR.csv"
BQ_DATASET  = "NFLverse_dataset"
BQ_TABLE    = "NFL_scraped_PFR"
BQ_LOCATION = "us-west1"

from google.cloud import storage
storage_client = storage.Client(project=GCP_PROJECT)
bucket = storage_client.bucket(GCS_BUCKET)
blob = bucket.blob(GCS_BLOB)
blob.upload_from_filename(local_csv)
gcs_uri = f"gs://{GCS_BUCKET}/{GCS_BLOB}"

from google.cloud import bigquery
bq = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)
ds_id = f"{GCP_PROJECT}.{BQ_DATASET}"
ds = bigquery.Dataset(ds_id)
ds.location = BQ_LOCATION
bq.get_dataset(ds_id)
table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
job = bq.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
job.result()