import os
import re
import json
import boto3
from botocore import UNSIGNED
from botocore.client import Config
from datetime import datetime, timedelta
from tqdm import tqdm
import subprocess
import hashlib
from pathlib import Path

# ---- CONFIG ----
S3_BUCKET = "indian-high-court-judgments-test"
S3_PREFIX = "metadata/json/"
LOCAL_DIR = "./local_hc_metadata"
DOWNLOAD_SCRIPT = "./download.py"
TRACK_FILE = "track.json"  # Used by download.py
AWS_PROFILE = "dattam-high-anshu"  # AWS profile for authentication

# ---- Upload/Merge CONFIG ----
OUTPUT_DIR = Path("./data")  # Change if needed, downloader output root
TEST_BUCKET = S3_BUCKET + "-test"

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

def validate_and_correct_json(court_code_dl, date):
    track_file_path = Path(TRACK_FILE)
    if not track_file_path.exists():
        print(f"[WARN] track.json not found, skipping validation.")
        return

    with open(track_file_path, 'r') as f:
        track_data = json.load(f)

    court_tracking = track_data.get(court_code_dl)
    if not court_tracking:
        print(f"[WARN] No tracking data for {court_code_dl}, skipping validation.")
        return

    # This is a bit of a hack, we don't know the exact file, so we'll look for the most recent
    # json file in the court's metadata directory for that day.
    date_str = date.strftime('%Y-%m-%d')
    year = date.strftime('%Y')
    court_code_s3 = court_code_dl.replace('~', '_')
    
    # Construct the path based on the S3 structure, but locally
    # Example: ./local_hc_metadata/year=2025/court=1_12/bench=kashmirhc/
    # We don't know the bench, so we have to search for it.
    
    base_path = Path(LOCAL_DIR) / f"year={year}" / f"court={court_code_s3}"
    if not base_path.exists():
        print(f"[WARN] Directory not found, cannot validate: {base_path}")
        return

    # Find all json files for the given date.
    all_json_files = []
    for bench_dir in base_path.iterdir():
        if bench_dir.is_dir():
            all_json_files.extend(list(bench_dir.glob(f'*{date_str}.json')))

    if not all_json_files:
        print(f"[WARN] No JSON files found for {court_code_dl} on {date_str} to validate.")
        return

    today = datetime.now().date()
    
    for json_file_path in all_json_files:
        print(f"[INFO] Validating {json_file_path}")
        with open(json_file_path, 'r+') as f:
            try:
                data = json.load(f)
                original_count = len(data)
                
                # The json file is a list of dicts
                corrected_data = []
                for record in data:
                    needs_correction = False
                    
                    # Check and correct decision_date
                    decision_date_str = record.get('decision_date')
                    if decision_date_str:
                        try:
                            decision_date = datetime.strptime(decision_date_str, '%d-%m-%Y').date()
                            if decision_date > today:
                                print(f"[WARN] Found future decision_date {decision_date} in record.")
                                needs_correction = True
                        except ValueError:
                            print(f"[WARN] Could not parse date '{decision_date_str}', keeping record.")
                    
                    # Check and correct pdf_link if it contains a future date
                    pdf_link = record.get('pdf_link')
                    if pdf_link:
                        # Look for patterns like HCBM050020952024_1_2028-03-28.pdf
                        pdf_date_match = re.search(r'_(\d{4})-(\d{2})-(\d{2})\.pdf$', pdf_link)
                        if pdf_date_match:
                            pdf_year = int(pdf_date_match.group(1))
                            pdf_month = int(pdf_date_match.group(2))
                            pdf_day = int(pdf_date_match.group(3))
                            
                            try:
                                pdf_date = datetime(pdf_year, pdf_month, pdf_day).date()
                                if pdf_date > today:
                                    print(f"[WARN] Found future date in pdf_link: {pdf_link}")
                                    
                                    # If we have a valid decision_date, use it to correct the pdf_link
                                    if decision_date_str and not needs_correction:
                                        decision_date = datetime.strptime(decision_date_str, '%d-%m-%Y').date()
                                        # Replace the future date in pdf_link with decision_date
                                        corrected_pdf_link = re.sub(
                                            r'_\d{4}-\d{2}-\d{2}\.pdf$',
                                            f'_{decision_date.strftime("%Y-%m-%d")}.pdf',
                                            pdf_link
                                        )
                                        print(f"[INFO] Corrected pdf_link: {pdf_link} -> {corrected_pdf_link}")
                                        record['pdf_link'] = corrected_pdf_link
                                    else:
                                        # If decision_date is also problematic, mark for removal
                                        needs_correction = True
                            except (ValueError, OverflowError):
                                print(f"[WARN] Invalid date in pdf_link: {pdf_link}, keeping record.")
                    
                    # Add record to corrected data if it doesn't need correction or we fixed it
                    if not needs_correction:
                        corrected_data.append(record)
                    else:
                        print(f"[INFO] Removing record with future date")

                if len(corrected_data) < original_count:
                    f.seek(0)
                    f.truncate()
                    json.dump(corrected_data, f, indent=4)
                    print(f"[INFO] Corrected {json_file_path}, removed {original_count - len(corrected_data)} records.")

            except json.JSONDecodeError:
                print(f"[WARN] Could not decode JSON from {json_file_path}, skipping.")

def run_downloader(court_code_dl, start_date):
    # end_date = start_date + timedelta(days=1)
    end_date = datetime.now().date()
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
    validate_and_correct_json(court_code_dl, start_date)

# --- UPLOAD/MERGE LOGIC ---
def file_md5(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def s3_key_from_local(local_path):
    return str(local_path.relative_to(OUTPUT_DIR)).replace("\\", "/")

def s3_object_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def upload_file(s3, local_path, bucket, key):
    s3.upload_file(str(local_path), bucket, key)
    print(f"Uploaded: {key}")

def merge_json(local_path, s3, bucket, key):
    tmp_remote = "remote_tmp.json"
    try:
        s3.download_file(bucket, key, tmp_remote)
        with open(tmp_remote, "r") as f:
            remote_data = json.load(f)
        os.remove(tmp_remote)
    except Exception:
        remote_data = []
    with open(local_path, "r") as f:
        local_data = json.load(f)
    # Merge: assumes list of dicts
    if isinstance(remote_data, dict): remote_data = [remote_data]
    if isinstance(local_data, dict): local_data = [local_data]
    merged = {json.dumps(item, sort_keys=True): item for item in remote_data + local_data}
    merged_list = list(merged.values())
    with open("merged_tmp.json", "w") as f:
        json.dump(merged_list, f)
    upload_file(s3, "merged_tmp.json", bucket, key)
    os.remove("merged_tmp.json")
    print(f"Merged JSON: {key}")

def merge_parquet(local_path, s3, bucket, key):
    import pyarrow as pa
    import pyarrow.parquet as pq
    tmp_remote = "remote_tmp.parquet"
    try:
        s3.download_file(bucket, key, tmp_remote)
        remote_table = pq.read_table(tmp_remote)
        os.remove(tmp_remote)
    except Exception:
        remote_table = None
    local_table = pq.read_table(local_path)
    if remote_table:
        merged_table = pa.concat_tables([remote_table, local_table])
    else:
        merged_table = local_table
    pq.write_table(merged_table, "merged_tmp.parquet")
    upload_file(s3, "merged_tmp.parquet", bucket, key)
    os.remove("merged_tmp.parquet")
    print(f"Merged Parquet: {key}")

def upload_and_merge_all():
    # Use the specified AWS profile for authentication
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3 = session.client("s3")
    
    for local_path in OUTPUT_DIR.rglob("*"):
        if local_path.is_dir(): continue
        key = s3_key_from_local(local_path)
        bucket = TEST_BUCKET
        print(f"Checking {key}...")
        exists = s3_object_exists(s3, bucket, key)
        # Merge/upload logic
        if local_path.suffix == ".json":
            if exists:
                merge_json(local_path, s3, bucket, key)
            else:
                upload_file(s3, local_path, bucket, key)
        elif local_path.suffix == ".parquet":
            if exists:
                merge_parquet(local_path, s3, bucket, key)
            else:
                upload_file(s3, local_path, bucket, key)
        elif local_path.suffix in [".pdf", ".tar", ".gz"]:
            if exists:
                tmp_remote = "remote_tmp.bin"
                try:
                    s3.download_file(bucket, key, tmp_remote)
                    remote_hash = file_md5(tmp_remote)
                    local_hash = file_md5(local_path)
                    os.remove(tmp_remote)
                    if remote_hash == local_hash:
                        print(f"Binary duplicate, skipping: {key}")
                        continue
                except Exception:
                    pass
            upload_file(s3, local_path, bucket, key)
        else:
            if not exists:
                upload_file(s3, local_path, bucket, key)

def main():
    year = datetime.now().year
    os.makedirs(LOCAL_DIR, exist_ok=True)
    
    # For listing data, we still use unsigned access to the main bucket
    s3_unsigned = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    # For operations that require credentials, use the profile
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3_auth = session.client("s3")

    test_court_code = "1_12"  # S3 format
    courts_and_benches = list_current_year_courts_and_benches(s3_unsigned, year)
    print(f"Found {len(courts_and_benches)} courts for year={year}")

    # Print the latest date for every court
    for court_code, bench_files in courts_and_benches.items():
        all_dates = set()
        for bench, files in bench_files.items():
            for key in files:
                m = re.search(r'(\d{4}-\d{2}-\d{2})\.json$', key)
                if m:
                    try:
                        d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                        # Skip future dates when reporting latest date
                        if d <= datetime.now().date():
                            all_dates.add(d)
                        else:
                            print(f"[WARN] Skipping future date {d} in filename {key}")
                    except Exception:
                        continue
        if all_dates:
            latest = max(all_dates)
            print(f"[LATEST] Court {court_code}: {latest}")
        else:
            print(f"[LATEST] Court {court_code}: No dates found")

    # Only process the test court code
    if test_court_code in courts_and_benches:
        try:
            bench_files = courts_and_benches[test_court_code]
            all_dates = set()
            for bench, files in bench_files.items():
                for key in files:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\.json$', key)
                    if m:
                        try:
                            d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                            # Only add dates from the current year to prevent processing old data
                            if d.year == year and d <= datetime.now().date():
                                all_dates.add(d)
                        except Exception:
                            continue
            
            if not all_dates:
                print(f"[WARN] No valid dates found for court={test_court_code}")
                return
                
            court_code_dl = test_court_code.replace('_', '~')
            
            # Debug to show all dates before processing
            print(f"[DEBUG] All dates for court {test_court_code}: {sorted(all_dates)}")
            
            # Process all dates for the test court, not just the earliest
            for dt in sorted(all_dates):
                run_downloader(court_code_dl, dt)
                
        except Exception as e:
            print(f"[ERROR] Failed to process court {test_court_code}: {str(e)}")
    else:
        print(f"[WARN] Test court code {test_court_code} not found in S3 data.")

    print("\nAll done. If new packages were generated, you may now proceed to upload to S3.")

    # ---- UPLOAD AND MERGE TO TEST BUCKET ----
    print("\nStarting upload/merge to test bucket...")
    upload_and_merge_all()
    print("Upload/merge complete.")

if __name__ == "__main__":
    main()