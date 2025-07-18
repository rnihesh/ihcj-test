import os
import re
import json
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from datetime import datetime, timedelta
from tqdm import tqdm
import subprocess

# ---- CONFIG ----
S3_BUCKET = "indian-high-court-judgments"
S3_PREFIX = "metadata/json/"
LOCAL_DIR = "./local_hc_metadata"
DOWNLOAD_SCRIPT = "./download.py"
TRACK_FILE = "track.json"  # Used by download.py

def list_current_year_courts_and_benches(s3, year):
    """
    Returns dict: { court_code: { bench: [json_files...] } }
    """
    prefix = f"{S3_PREFIX}year={year}/"
    paginator = s3.get_paginator("list_objects_v2")
    result = {}

    print(f"DEBUG: Paginating S3 with Prefix: {prefix}")
    total_keys = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        contents = page.get("Contents", [])
        print(f"DEBUG: Got {len(contents)} keys in one page")
        for obj in contents:
            key = obj["Key"]
            total_keys += 1
            # key example: metadata/json/year=2025/court=1_12/bench=kashmirhc/JKHC010046902008_1_2020-09-21.json
            m = re.match(
                rf"{re.escape(S3_PREFIX)}year={year}/court=([^/]+)/bench=([^/]+)/([^/]+\.json)$",
                key
            )
            if not m:
                continue
            court_code, bench, json_file = m.group(1), m.group(2), m.group(3)
            result.setdefault(court_code, {}).setdefault(bench, []).append(key)
    print(f"DEBUG: Total keys found under year={year}: {total_keys}")
    print(f"DEBUG: Courts/benches loaded: {result.keys()}")
    return result

def get_latest_decision_date_for_court(court_bench_files):
    """
    Finds the latest decision date for a court by extracting from filenames (fast!).
    """
    latest_date = None
    for bench, files in court_bench_files.items():
        for key in files:
            # Expect date at end of filename before .json (YYYY-MM-DD)
            m = re.search(r'(\d{4}-\d{2}-\d{2})\.json$', key)
            if m:
                try:
                    d = datetime.strptime(m.group(1), "%Y-%m-%d")
                    if latest_date is None or d > latest_date:
                        latest_date = d
                except Exception:
                    continue
    return latest_date

def run_downloader(court_code_dl, start_date, end_date):
    # Always set end_date to start_date + 1 day
    end_date = start_date + timedelta(days=1)
    print(f"\nRunning downloader for court={court_code_dl} from {start_date} to {end_date} ...")
    cmd = [
        "python", DOWNLOAD_SCRIPT,
        "--court_code", court_code_dl,
        "--start_date", start_date.strftime("%Y-%m-%d"),
        "--end_date", end_date.strftime("%Y-%m-%d"),
        "--day_step", "2"
    ]
    print("Command:", " ".join(cmd))
    subprocess.run(cmd, check=True)

def main():
    year = datetime.now().year
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    courts_and_benches = list_current_year_courts_and_benches(s3, year)
    print(f"Found {len(courts_and_benches)} courts for year={year}")

    today = datetime.now().date()
    update_plan = []

    for court_code, bench_files in tqdm(courts_and_benches.items(), desc="Scanning courts"):
        latest_date = get_latest_decision_date_for_court(bench_files)
        if not latest_date:
            print(f"[WARN] No decision dates found for court {court_code}, skipping.")
            continue
        print(f"[INFO] Latest decision for court={court_code}: {latest_date.date()}")
        if latest_date.date() < today:
            # Convert court_code from S3 (with _) to download.py format (with ~)
            court_code_dl = court_code.replace('_', '~')
            update_plan.append((court_code_dl, latest_date.date(), today))
        else:
            print(f"[OK] No new data to fetch for court={court_code}")

    for court_code_dl, last_dt, today_dt in update_plan:
        run_downloader(court_code_dl, last_dt, today_dt)

    print("\nAll done. If new packages were generated, you may now proceed to upload to S3.")

if __name__ == "__main__":
    main()