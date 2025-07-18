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

def run_downloader(court_code_dl, start_date):
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

    test_court_code = "1_12"  # S3 format
    courts_and_benches = list_current_year_courts_and_benches(s3, year)
    print(f"Found {len(courts_and_benches)} courts for year={year}")

    # Only process the test court code
    if test_court_code in courts_and_benches:
        bench_files = courts_and_benches[test_court_code]
        # Find all dates from all benches
        all_dates = set()
        for bench, files in bench_files.items():
            for key in files:
                m = re.search(r'(\d{4}-\d{2}-\d{2})\.json$', key)
                if m:
                    try:
                        d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                        all_dates.add(d)
                    except Exception:
                        continue
        if not all_dates:
            print(f"[WARN] No dates found for court={test_court_code}")
            return
        earliest_date = min(all_dates)
        print(f"[INFO] Earliest date for court={test_court_code} is {earliest_date}")

        # Run downloader for only the earliest date
        court_code_dl = test_court_code.replace('_', '~')
        run_downloader(court_code_dl, earliest_date)
    else:
        print(f"[WARN] Test court code {test_court_code} not found in S3 data.")

    print("\nAll done. If new packages were generated, you may now proceed to upload to S3.")

if __name__ == "__main__":
    main()