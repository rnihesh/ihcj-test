from typing import Optional, Generator
from tqdm import tqdm
import argparse
from datetime import datetime, timedelta
import traceback
import re
import json
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import lxml.html as LH
from http.cookies import SimpleCookie
import urllib
import easyocr
import logging
import threading
import concurrent.futures
import urllib3
import uuid
import os
import hashlib
import shutil

# S3 imports - only imported when needed
try:
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config
    S3_AVAILABLE = True
except ImportError as e:
    print(f"❌ S3 dependencies not available: {e}")
    S3_AVAILABLE = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from process_metadata import MetadataProcessor
    PARQUET_AVAILABLE = True
except ImportError as e:
    print(f"❌ Parquet dependencies not available: {e}")
    PARQUET_AVAILABLE = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# add a logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

reader = easyocr.Reader(["en"])

root_url = "https://judgments.ecourts.gov.in"
output_dir = Path("./data")
START_DATE = "2008-01-01"


def format_size(size_bytes):
    """Format bytes into human readable string"""
    if size_bytes == 0:
        return "0 B"
    
    size_units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    unit_index = 0
    
    while size >= 1024.0 and unit_index < len(size_units) - 1:
        size /= 1024.0
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {size_units[unit_index]}"
    else:
        return f"{size:.2f} {size_units[unit_index]}"


payload = "&sEcho=1&iColumns=2&sColumns=,&iDisplayStart=0&iDisplayLength=100&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=asc&iSortingCols=1&search_txt1=&search_txt2=&search_txt3=&search_txt4=&search_txt5=&pet_res=&state_code=27~1&state_code_li=&dist_code=null&case_no=&case_year=&from_date=&to_date=&judge_name=&reg_year=&fulltext_case_type=&int_fin_party_val=undefined&int_fin_case_val=undefined&int_fin_court_val=undefined&int_fin_decision_val=undefined&act=&sel_search_by=undefined&sections=undefined&judge_txt=&act_txt=&section_txt=&judge_val=&act_val=&year_val=&judge_arr=&flag=&disp_nature=&search_opt=PHRASE&date_val=ALL&fcourt_type=2&citation_yr=&citation_vol=&citation_supl=&citation_page=&case_no1=&case_year1=&pet_res1=&fulltext_case_type1=&citation_keyword=&sel_lang=&proximity=&neu_cit_year=&neu_no=&ajax_req=true&app_token=1fbc7fbb840eb95975c684565909fe6b3b82b8119472020ff10f40c0b1c901fe"


pdf_link_payload = "val=0&lang_flg=undefined&path=cnrorders/taphc/orders/2017/HBHC010262202017_1_2047-06-29.pdf#page=&search=+&citation_year=&fcourt_type=2&file_type=undefined&nc_display=undefined&ajax_req=true&app_token=c64944b84c687f501f9692e239e2a0ab007eabab497697f359a2f62e4fcd3d10"

page_size = 5000
MATH_CAPTCHA = False
NO_CAPTCHA_BATCH_SIZE = 25
lock = threading.Lock()

captcha_failures_dir = Path("./captcha-failures")
captcha_tmp_dir = Path("./captcha-tmp")
captcha_failures_dir.mkdir(parents=True, exist_ok=True)
captcha_tmp_dir.mkdir(parents=True, exist_ok=True)

# ---- S3 SYNC CONFIG ----
S3_BUCKET = "indian-high-court-judgments-test"
S3_PREFIX = "metadata/json/"
LOCAL_DIR = "./local_hc_metadata"
BENCH_CODES_FILE = "bench-codes.json"
OUTPUT_DIR = output_dir  # Reuse existing output_dir


def get_json_file(file_path) -> dict:
    with open(file_path) as f:
        return json.load(f)


def get_court_codes():
    court_codes = get_json_file("./court-codes.json")
    return court_codes


def get_tracking_data():
    tracking_data = get_json_file("./track.json")
    return tracking_data


def save_tracking_data(tracking_data):
    with open("./track.json", "w") as f:
        json.dump(tracking_data, f)


def save_court_tracking_date(court_code, court_tracking):
    # acquire a lock
    lock.acquire()
    tracking_data = get_tracking_data()
    tracking_data[court_code] = court_tracking
    save_tracking_data(tracking_data)
    # release the lock
    lock.release()


def get_new_date_range(
    last_date: str, day_step: int = 1
) -> tuple[str | None, str | None]:
    last_date_dt = datetime.strptime(last_date, "%Y-%m-%d")
    new_from_date_dt = last_date_dt + timedelta(days=1)
    new_to_date_dt = new_from_date_dt + timedelta(days=day_step - 1)
    if new_from_date_dt.date() > datetime.now().date():
        return None, None

    if new_to_date_dt.date() > datetime.now().date():
        new_to_date_dt = datetime.now().date()
    new_from_date = new_from_date_dt.strftime("%Y-%m-%d")
    new_to_date = new_to_date_dt.strftime("%Y-%m-%d")
    return new_from_date, new_to_date


def get_date_ranges_to_process(court_code, start_date=None, end_date=None, day_step=1):
    """
    Generate date ranges to process for a given court.
    If start_date is provided but no end_date, use current date as end_date.
    If neither is provided, use tracking data to determine the next date range.
    """
    # If start_date is provided but end_date is not, use current date as end_date
    if start_date and not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date and end_date:
        # Convert string dates to datetime objects
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Generate date ranges with specified step
        current_date = start_date_dt
        while current_date <= end_date_dt:
            range_end = min(current_date + timedelta(days=day_step - 1), end_date_dt)
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            current_date = range_end + timedelta(days=1)
    else:
        # Use tracking data to get next date range
        tracking_data = get_tracking_data()
        court_tracking = tracking_data.get(court_code, {})
        last_date = court_tracking.get("last_date", START_DATE)

        # Process from last_date to current date in chunks
        current_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        end_date_dt = datetime.now()

        while current_date <= end_date_dt:
            range_end = min(current_date + timedelta(days=day_step - 1), end_date_dt)
            yield (current_date.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"))
            current_date = range_end + timedelta(days=1)


class CourtDateTask:
    """A task representing a court and date range to process"""

    def __init__(self, court_code, from_date, to_date):
        self.id = str(uuid.uuid4())
        self.court_code = court_code
        self.from_date = from_date
        self.to_date = to_date

    def __str__(self):
        return f"CourtDateTask(id={self.id}, court_code={self.court_code}, from_date={self.from_date}, to_date={self.to_date})"


def generate_tasks(
    court_codes: Optional[list[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    day_step: int = 1,
) -> Generator[CourtDateTask, None, None]:
    """Generate tasks for processing courts and date ranges as a generator"""
    all_court_codes = get_court_codes()
    if not court_codes:
        court_codes = all_court_codes
    else:
        court_codes = {
            court_code: all_court_codes[court_code] for court_code in court_codes
        }

    for code in court_codes:
        for from_date, to_date in get_date_ranges_to_process(
            code, start_date, end_date, day_step
        ):
            yield CourtDateTask(code, from_date, to_date)


def process_task(task):
    """Process a single court-date task"""
    try:
        downloader = Downloader(task)
        downloader.download()
    except Exception as e:
        court_codes = get_court_codes()
        logger.error(
            f"Error processing court {task.court_code} {court_codes.get(task.court_code, 'Unknown')}: {e}"
        )
        traceback.print_exc()


def run(court_codes=None, start_date=None, end_date=None, day_step=1, max_workers=2):
    """
    Run the downloader with optional parameters using Python's multiprocessing
    with a generator that yields tasks on demand.
    """
    # Create a task generator and convert to list to get total count
    print("Generating tasks...")
    tasks = list(generate_tasks(court_codes, start_date, end_date, day_step))
    print(f"Generated {len(tasks)} tasks to process")
    
    if not tasks:
        logger.info("No tasks to process")
        return

    # Use ProcessPoolExecutor with map to process tasks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Use tqdm to show progress
        with tqdm(total=len(tasks), desc="Processing tasks", unit="task") as pbar:
            for i, result in enumerate(executor.map(process_task, tasks)):
                # process_task doesn't return anything, so we're just tracking progress
                task = tasks[i]
                pbar.set_description(f"Processing {task.court_code} ({task.from_date} to {task.to_date})")
                pbar.update(1)

    logger.info("All tasks completed")


# ---- S3 SYNC FUNCTIONS ----
def get_bench_codes():
    """Load bench to court mappings from bench-codes.json"""
    try:
        with open(BENCH_CODES_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[WARN] {BENCH_CODES_FILE} not found, using empty mapping")
        return {}


def get_court_dates_from_index_files():
    """Get updated_at dates from metadata index files"""
    if not S3_AVAILABLE:
        print("[ERROR] S3 not available")
        return {}
    
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    year = datetime.now().year
    prefix = f"metadata/tar/year={year}/"
    
    print(f"Reading dates from index files: {S3_BUCKET}/{prefix}")
    
    result = {}
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("metadata.index.json"):
                    continue
                    
                # Extract court and bench from path
                court_code, bench = extract_court_bench_from_path(key)
                if not court_code or not bench:
                    continue
                
                # Get updated_at from index file
                updated_at = read_updated_at_from_index(s3, S3_BUCKET, key)
                if updated_at:
                    if court_code not in result:
                        result[court_code] = {}
                    result[court_code][bench] = updated_at
    
    except Exception as e:
        print(f"[ERROR] Failed to fetch dates: {e}")
        return {}
    
    print(f"Found dates for {len(result)} courts")
    return result


def extract_court_bench_from_path(key):
    """Extract court and bench from S3 key path"""
    parts = key.split('/')
    court_code = bench = None
    
    for part in parts:
        if part.startswith('court='):
            court_code = part[6:]  # Remove 'court=' prefix
        elif part.startswith('bench='):
            bench = part[6:]  # Remove 'bench=' prefix
    
    return court_code, bench


def read_updated_at_from_index(s3, bucket, key):
    """Get updated_at timestamp from index file"""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        index_data = json.loads(response['Body'].read().decode('utf-8'))
        return index_data.get('updated_at')
    except Exception as e:
        print(f"[WARN] Failed to read {key}: {e}")
        return None


def update_index_files_after_download(court_code, bench, new_files):
    """Update both metadata and data index files with new download information"""
    if not S3_AVAILABLE:
        print("[ERROR] S3 not available")
        return
    
    s3_client = boto3.client('s3')
    s3_unsigned = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    year = datetime.now().year
    current_time = datetime.now().isoformat()
    
    # Update both metadata and data index files
    updates = [
        {
            'type': 'metadata',
            'key': f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.index.json",
            'tar_key': f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.tar.gz",
            'files': new_files.get('metadata', [])
        },
        {
            'type': 'data', 
            'key': f"data/tar/year={year}/court={court_code}/bench={bench}/data.index.json",
            'tar_key': f"data/tar/year={year}/court={court_code}/bench={bench}/pdfs.tar",
            'files': new_files.get('data', [])
        }
    ]
    
    for update in updates:
        if not update['files']:
            continue
            
        try:
            # Read existing index or create new one
            try:
                response = s3_unsigned.get_object(Bucket=S3_BUCKET, Key=update['key'])
                index_data = json.loads(response['Body'].read().decode('utf-8'))
            except Exception:
                index_data = {"files": [], "file_count": 0, "updated_at": current_time}
            
            # Add new files
            existing_files = set(index_data.get('files', []))
            for new_file in update['files']:
                if new_file not in existing_files:
                    index_data['files'].append(new_file)
            
            # Update metadata
            index_data['file_count'] = len(index_data['files'])
            index_data['updated_at'] = current_time
            
            # Get actual tar file size
            try:
                tar_response = s3_unsigned.head_object(Bucket=S3_BUCKET, Key=update['tar_key'])
                tar_size = tar_response['ContentLength']
                index_data['tar_size'] = tar_size
                index_data['tar_size_human'] = format_size(tar_size)
            except Exception as e:
                print(f"[WARN] Could not get tar size for {update['tar_key']}: {e}")
                index_data['tar_size'] = 0
                index_data['tar_size_human'] = "0 B"
            
            # Write back to S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=update['key'],
                Body=json.dumps(index_data, indent=2),
                ContentType='application/json'
            )
            print(f"Updated {update['type']} index with {len(update['files'])} files")
            
        except Exception as e:
            print(f"Failed to update {update['type']} index: {e}")


def run_incremental_download(court_code_dl, start_date, end_date=None):
    """Run download for specific court and date range"""
    # If no end_date provided, make it same as start_date
    if end_date is None:
        end_date = start_date
    
    print(f"Downloading {court_code_dl}: {start_date} → {end_date}")
    
    try:
        # Create and process download task
        task = CourtDateTask(
            court_code=court_code_dl,
            # from_date=start_date.strftime("%Y-%m-%d"),
            # to_date=end_date.strftime("%Y-%m-%d")
            from_date="2025-01-01",
            to_date="2025-09-09"
        )
        
        # Track downloaded files
        downloaded_files = {'metadata': [], 'data': []}
        
        # Create a modified downloader that tracks files and forces PDF downloads
        downloader = FileTrackingDownloader(task, downloaded_files, force_pdf_download=False)
        downloader.download()
        
        print(f"Download completed: {court_code_dl}")
        print(f"Downloaded {len(downloaded_files['metadata'])} metadata files and {len(downloaded_files['data'])} PDF files")
        
        return downloaded_files
        
    except Exception as e:
        print(f"Download failed {court_code_dl}: {e}")
        return {'metadata': [], 'data': []}


def sync_to_s3(test_mode=False, court_code=None):
    """Sync new data to S3 using index files for date tracking"""
    if not S3_AVAILABLE:
        print("[ERROR] S3 dependencies not available")
        return
    
    # Use provided court code or default to 11~24 for testing
    if court_code is None:
        court_code = "11~24"  # Default test court
    
    print(f"\n[SYNC S3] Processing court {court_code}")
    
    # Get current dates from S3 index files
    court_dates = get_court_dates_from_index_files()
    
    # Convert court code for S3 lookup (replace ~ with _)
    s3_court_code = court_code.replace('~', '_')
    
    benches = court_dates.get(s3_court_code, {})
    if not benches:
        print(f"No existing data found for court {court_code}, will download from beginning")
        # If no existing data, download recent data
        start_date = datetime.now().date() - timedelta(days=30)  # Last 30 days
        downloaded_files = run_incremental_download(court_code, start_date, datetime.now().date())
    else:
        # Use existing download_court_data logic which handles updated_at properly
        latest_date = get_latest_court_date(benches)
        print(f"Latest date for court {court_code}: {latest_date}")
        downloaded_files = download_court_data(s3_court_code, latest_date, test_mode)
        
    if downloaded_files and (downloaded_files['metadata'] or downloaded_files['data']):
        upload_files_to_s3(court_code, downloaded_files)
    print(f"\nSync completed for court {court_code}")


def get_latest_court_date(benches):
    """Get the most recent date from all benches"""
    latest_date = None
    for bench, updated_at_str in benches.items():
        try:
            date = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00')).date()
            if latest_date is None or date > latest_date:
                latest_date = date
        except Exception as e:
            print(f"[WARN] Invalid date {updated_at_str}: {e}")
    return latest_date


def download_court_data(court_code, latest_date, test_mode=False):
    """Download data for a specific court"""
    today = datetime.now().date()
    
    # Check if we need to download anything
    if latest_date >= today:
        print(f"Court {court_code}: Already up-to-date (latest: {latest_date})")
        return {'metadata': [], 'data': []}
    
    # Convert court code for download (replace _ with ~)
    court_code_dl = court_code.replace('_', '~')
    
    # Calculate the next date to download (latest_date + 1)
    start_date = latest_date + timedelta(days=1)
    
    if test_mode:
        # Test mode: download only 1 day
        end_date = start_date
        print(f"[TEST MODE] Downloading: Court {court_code}: {start_date} (1 day only)")
    else:
        # Production mode: download from next day up to today
        end_date = today
        print(f"Downloading: Court {court_code}: {start_date} → {end_date}")
    
    # Don't download future dates
    if start_date > today:
        print(f"Court {court_code}: No future data to download")
        return {'metadata': [], 'data': []}
    
    # Run download
    downloaded_files = run_incremental_download(court_code_dl, start_date, end_date)
    
    # Update all bench index files
    court_dates = get_court_dates_from_index_files()
    benches = court_dates.get(court_code, {})
    for bench in benches.keys():
        update_index_files_after_download(court_code, bench, downloaded_files)
    
    # Return the downloaded files so caller can handle upload
    return downloaded_files


class Downloader:
    def __init__(self, task: CourtDateTask):
        self.task = task
        self.root_url = "https://judgments.ecourts.gov.in"
        self.search_url = f"{self.root_url}/pdfsearch/?p=pdf_search/home/"
        self.captcha_url = f"{self.root_url}/pdfsearch/vendor/securimage/securimage_show.php"  # not lint skip/
        self.captcha_token_url = f"{self.root_url}/pdfsearch/?p=pdf_search/checkCaptcha"
        self.pdf_link_url = f"{self.root_url}/pdfsearch/?p=pdf_search/openpdfcaptcha"
        self.pdf_link_url_wo_captcha = f"{root_url}/pdfsearch/?p=pdf_search/openpdf"

        self.court_code = task.court_code
        self.tracking_data = get_tracking_data()
        self.court_codes = get_court_codes()
        self.court_name = self.court_codes[self.court_code]
        self.court_tracking = self.tracking_data.get(self.court_code, {})
        self.session_cookie_name = "JUDGEMENTSSEARCH_SESSID"
        self.ecourts_token_cookie_name = "JSESSION"
        self.session_id = None
        self.ecourts_token = None
        self.app_token = "490a7e9b99e4553980213a8b86b3235abc51612b038dbdb1f9aa706b633bbd6c"  # not lint skip/

    def _results_exist_in_search_response(self, res_dict):

        results_exist = (
            "reportrow" in res_dict
            and "aaData" in res_dict["reportrow"]
            and len(res_dict["reportrow"]["aaData"]) > 0
        )
        if results_exist:
            no_of_results = len(res_dict["reportrow"]["aaData"])
            logger.info(f"Found {no_of_results} results for task: {self.task}")
        return results_exist

    def _prepare_next_iteration(self, search_payload):
        search_payload["sEcho"] += 1
        search_payload["iDisplayStart"] += page_size
        logger.info(
            f"Next iteration: {search_payload['iDisplayStart']}, task: {self.task.id}"
        )
        return search_payload

    def process_court(self):
        last_date = self.court_tracking.get("last_date", START_DATE)
        from_date, to_date = get_new_date_range(last_date)
        if from_date is None:
            logger.info(f"No more data to download for: task: {self.task.id}")
            return
        search_payload = self.default_search_payload()
        search_payload["from_date"] = from_date
        search_payload["to_date"] = to_date
        self.init_user_session()
        search_payload["state_code"] = self.court_code
        search_payload["app_token"] = self.app_token
        results_available = True
        pdfs_downloaded = 0

        while results_available:
            try:
                response = self.request_api("POST", self.search_url, search_payload)
                res_dict = response.json()
                if self._results_exist_in_search_response(res_dict):

                    for idx, row in enumerate(res_dict["reportrow"]["aaData"]):
                        try:
                            is_pdf_downloaded = self.process_result_row(
                                row, row_pos=idx
                            )
                            if is_pdf_downloaded:
                                pdfs_downloaded += 1
                            else:
                                self.court_tracking["failed_dates"] = (
                                    self.court_tracking.get("failed_dates", [])
                                )
                                if from_date not in self.court_tracking["failed_dates"]:
                                    self.court_tracking["failed_dates"].append(
                                        from_date
                                    )
                            if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                                # after 25 downloads, need to solve captcha for every pdf link request. Starting with a fresh session would be faster so that we get another 25 downloads without captcha
                                logger.info(
                                    f"Downloaded {NO_CAPTCHA_BATCH_SIZE} pdfs, starting with fresh session, task: {self.task.id}"
                                )
                                break

                        except Exception as e:
                            logger.error(
                                f"Error processing row {row}: {e}, task: {self.task}"
                            )
                            traceback.print_exc()
                    if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                        pdfs_downloaded = 0
                        self.init_user_session()
                        search_payload["app_token"] = self.app_token
                        continue
                        # we are skipping the rest of the loop, meaning we fetch the 1000 results again for the same page, with a new session and process. Already downloaded pdfs will be skipped. This continues until we hve downloaded the whole page.
                    # prepare next iteration
                    search_payload = self._prepare_next_iteration(search_payload)
                else:
                    last_date = to_date
                    self.court_tracking["last_date"] = last_date
                    save_court_tracking_date(self.court_code, self.court_tracking)
                    from_date, to_date = get_new_date_range(to_date)
                    if from_date is None:
                        logger.info(f"No more data to download for: task: {self.task}")
                        results_available = False
                    else:
                        search_payload["from_date"] = from_date
                        search_payload["to_date"] = to_date
                        search_payload["sEcho"] = 1
                        search_payload["iDisplayStart"] = 0
                        search_payload["iDisplayLength"] = page_size
                        logger.info(f"Downloading data for task: {self.task}")

            except Exception as e:
                logger.error(f"Error processing task: {self.task}, Error: {e}")
                traceback.print_exc()
                self.court_tracking["failed_dates"] = self.court_tracking.get(
                    "failed_dates", []
                )
                if from_date not in self.court_tracking["failed_dates"]:
                    self.court_tracking["failed_dates"].append(
                        from_date
                    )  # TODO: should be all the dates from from_date to to_date in case step date > 1
                save_court_tracking_date(self.court_code, self.court_tracking)

    def process_result_row(self, row, row_pos):
        html = row[1]
        soup = BeautifulSoup(html, "html.parser")
        # html_element = LH.fromstring(html)
        # why am I using both LH and BS4? idk.
        # title = html_element.xpath("./button/font/text()")[0]
        # description = html_element.xpath("./text()")[0]
        # case_details = html_element.xpath("./strong//text()")
        # check if button with onclick is present
        if not (soup.button and "onclick" in soup.button.attrs):
            logger.info(
                f"No button found, likely multi language judgment, task: {self.task}"
            )
            with open("html-parse-failures.txt", "a") as f:
                f.write(html + "\n")
            # TODO: requires special parsing
            return False
        pdf_fragment = self.extract_pdf_fragment(soup.button["onclick"])
        pdf_output_path = self.get_pdf_output_path(pdf_fragment)
        is_pdf_present = self.is_pdf_downloaded(pdf_fragment)
        pdf_needs_download = not is_pdf_present
        if pdf_needs_download:
            is_fresh_download = self.download_pdf(pdf_fragment, row_pos)
        else:
            is_fresh_download = False
        metadata_output = pdf_output_path.with_suffix(".json")
        metadata = {
            "court_code": self.court_code,
            "court_name": self.court_name,
            "raw_html": html,
            # "title": title,
            # "description": description,
            # "case_details": case_details,
            "pdf_link": pdf_fragment,
            "downloaded": is_pdf_present or is_fresh_download,
        }
        metadata_output.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_output, "w") as f:
            json.dump(metadata, f)
        return is_fresh_download

    def download_pdf(self, pdf_fragment, row_pos):
        # prepare temp pdf request
        pdf_output_path = self.get_pdf_output_path(pdf_fragment)
        pdf_link_payload = self.default_pdf_link_payload()
        pdf_link_payload["path"] = pdf_fragment
        pdf_link_payload["val"] = row_pos
        pdf_link_payload["app_token"] = self.app_token
        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url, pdf_link_payload
        )
        if "outputfile" not in pdf_link_response.json():
            logger.error(
                f"Error downloading pdf, task: {self.task}, Error: {pdf_link_response.json()}"
            )
            return False
        pdf_download_link = pdf_link_response.json()["outputfile"]

        # download pdf and save
        pdf_response = requests.request(
            "GET",
            root_url + pdf_download_link,
            verify=False,
            headers=self.get_headers(),
            timeout=30,
        )
        pdf_output_path.parent.mkdir(parents=True, exist_ok=True)
        # number of response butes
        no_of_bytes = len(pdf_response.content)
        if no_of_bytes == 0:
            logger.error(
                f"Empty pdf, task: {self.task}, output path: {pdf_output_path}"
            )
            return False
        if no_of_bytes == 315:
            logger.error(
                f"404 pdf response, task: {self.task}, output path: {pdf_output_path}"
            )
            return False
        with open(pdf_output_path, "wb") as f:
            f.write(pdf_response.content)
        logger.debug(
            f"Downloaded, task: {self.task}, output path: {pdf_output_path}, size: {no_of_bytes}"
        )
        return True

    def update_headers_with_new_session(self, headers):
        cookie = SimpleCookie()
        cookie.load(headers["Cookie"])
        cookie[self.session_cookie_name] = self.session_id
        headers["Cookie"] = cookie.output(header="", sep=";").strip()

    def extract_pdf_fragment(self, html_attribute):
        pattern = r"javascript:open_pdf\('.*?','.*?','(.*?)'\)"
        match = re.search(pattern, html_attribute)
        if match:
            return match.group(1).split("#")[0]
        return None

    def solve_math_expression(self, expression):
        # credits to: https://github.com/NoelShallum
        expression = expression.strip().replace(" ", "").replace(".", "")
        if "+" in expression:
            nums = expression.split("+")
            return str(int(nums[0]) + int(nums[1]))
        elif "-" in expression:
            nums = expression.split("-")
            return str(int(nums[0]) - int(nums[1]))
        elif (
            "*" in expression
            or "X" in expression
            or "x" in expression
            or "×" in expression
        ):
            expression = (
                expression.replace("x", "*").replace("×", "*").replace("X", "*")
            )
            nums = expression.split("*")
            return str(int(nums[0]) * int(nums[1]))
        elif "/" in expression or "÷" in expression:
            expression = expression.replace("÷", "/")
            nums = expression.split("/")
            return str(int(nums[0]) // int(nums[1]))
        else:
            raise ValueError(f"Unsupported mathematical expression: {expression}")

    def is_math_expression(self, expression):
        separators = ["+", "-", "*", "/", "÷", "x", "×", "X"]
        for separator in separators:
            if separator in expression:
                return True
        return False

    def solve_captcha(self, retries=0, captcha_url=None):
        logger.debug(f"Solving captcha, retries: {retries}, task: {self.task.id}")
        if retries > 10:
            raise ValueError("Failed to solve captcha")
        if captcha_url is None:
            captcha_url = self.captcha_url
        # download captcha image and save
        captcha_response = requests.get(
            captcha_url, headers={"Cookie": self.get_cookie()}, verify=False, timeout=30
        )
        # Generate a unique filename using UUID
        unique_id = uuid.uuid4().hex[:8]
        captcha_filename = Path(
            f"{captcha_tmp_dir}/captcha_{self.court_code}_{unique_id}.png"
        )
        with open(captcha_filename, "wb") as f:
            f.write(captcha_response.content)
        result = reader.readtext(str(captcha_filename))
        if not result:
            logger.warning(
                f"No result from captcha, task: {self.task.id}, retries: {retries}"
            )
            return self.solve_captcha(retries + 1, captcha_url)
        captch_text = result[0][1].strip()

        if MATH_CAPTCHA:
            if self.is_math_expression(captch_text):
                try:
                    answer = self.solve_math_expression(captch_text)
                    captcha_filename.unlink()
                    return answer
                except Exception as e:
                    logger.error(
                        f"Error solving math expression, task: {self.task.id}, retries: {retries}, captcha text: {captch_text}, Error: {e}"
                    )
                    # move the captcha image to a new folder for debugging
                    new_filename = f"{uuid.uuid4().hex[:8]}_{captcha_filename.name}"
                    captcha_filename.rename(
                        Path(f"{captcha_failures_dir}/{new_filename}")
                    )
                    return self.solve_captcha(retries + 1, captcha_url)
            else:
                # If not a math expression, try again
                captcha_filename.unlink()  # Clean up the file
                return self.solve_captcha(retries + 1, captcha_url)
        else:
            captcha_text = "".join([c for c in captch_text if c.isalnum()])
            if len(captcha_text) != 6:
                if retries > 10:
                    raise Exception("Captcha not solved")
                return self.solve_captcha(retries + 1)
            return captcha_text

    def solve_pdf_download_captcha(self, response, pdf_link_payload, retries=0):
        html_str = response["filename"]
        html = LH.fromstring(html_str)
        img_src = html.xpath("//img[@id='captcha_image_pdf']/@src")[0]
        img_src = root_url + img_src
        # download captch image and save
        captcha_text = self.solve_captcha(captcha_url=img_src)
        pdf_link_payload["captcha1"] = captcha_text
        pdf_link_payload["app_token"] = response["app_token"]
        pdf_link_response = self.request_api(
            "POST", self.pdf_link_url_wo_captcha, pdf_link_payload
        )
        res_json = pdf_link_response.json()
        if "message" in res_json and res_json["message"] == "Captcha not solved":
            logger.warning(
                f"Captcha not solved, task: {self.task.id}, retries: {retries}, Error: {pdf_link_response.json()}"
            )
            if retries == 2:
                return res_json
            logger.info(f"Retrying pdf captch solve, task: {self.task.id}")
            return self.solve_pdf_download_captcha(
                response, pdf_link_payload, retries + 1
            )
        return pdf_link_response

    def refresh_token(self, with_app_token=False):
        logger.debug(f"Current session id {self.session_id}, token {self.app_token}")
        answer = self.solve_captcha()
        captcha_check_payload = {
            "captcha": answer,
            "search_opt": "PHRASE",
            "ajax_req": "true",
        }
        if with_app_token:
            captcha_check_payload["app_token"] = self.app_token
        res = requests.request(
            "POST",
            self.captcha_token_url,
            headers=self.get_headers(),
            data=captcha_check_payload,
            verify=False,
            timeout=30,
        )
        res_json = res.json()
        self.app_token = res_json["app_token"]
        self.update_session_id(res)
        logger.debug("Refreshed token")

    def request_api(self, method, url, payload, **kwargs):
        headers = self.get_headers()
        logger.debug(
            f"api_request {self.session_id} {payload.get('app_token') if payload else None} {url}"
        )
        response = requests.request(
            method,
            url,
            headers=headers,
            data=payload,
            **kwargs,
            timeout=60,
            verify=False,
        )
        # if response is json
        try:
            response_dict = response.json()
        except Exception:
            response_dict = {}
        if "app_token" in response_dict:
            self.app_token = response_dict["app_token"]
        self.update_session_id(response)
        if url == self.captcha_token_url:
            return response

        if (
            "filename" in response_dict
            and "securimage_show" in response_dict["filename"]
        ):
            self.app_token = response_dict["app_token"]
            return self.solve_pdf_download_captcha(response_dict, payload)

        elif response_dict.get("session_expire") == "Y":
            self.refresh_token()
            if payload:
                payload["app_token"] = self.app_token
            return self.request_api(method, url, payload, **kwargs)

        elif "errormsg" in response_dict:
            logger.error(f"Error {response_dict['errormsg']}")
            self.refresh_token()
            if payload:
                payload["app_token"] = self.app_token
            return self.request_api(method, url, payload, **kwargs)

        return response

    def get_pdf_output_path(self, pdf_fragment):
        return output_dir / pdf_fragment.split("#")[0]

    def is_pdf_downloaded(self, pdf_fragment):
        pdf_metadata_path = self.get_pdf_output_path(pdf_fragment).with_suffix(".json")
        if pdf_metadata_path.exists():
            pdf_metadata = get_json_file(pdf_metadata_path)
            return pdf_metadata["downloaded"]
        return False

    def get_search_url(self):
        return f"{self.root_url}/pdfsearch/?p=pdf_search/home/"

    def default_search_payload(self):
        search_payload = urllib.parse.parse_qs(payload)
        search_payload = {k: v[0] for k, v in search_payload.items()}
        search_payload["sEcho"] = 1
        search_payload["iDisplayStart"] = 0
        search_payload["iDisplayLength"] = page_size
        return search_payload

    def default_pdf_link_payload(self):
        pdf_link_payload_o = urllib.parse.parse_qs(pdf_link_payload)
        pdf_link_payload_o = {k: v[0] for k, v in pdf_link_payload_o.items()}
        return pdf_link_payload_o

    def init_user_session(self):
        res = requests.request(
            "GET",
            f"{self.root_url}/pdfsearch/",
            verify=False,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            },
            timeout=30,
        )
        self.session_id = res.cookies.get(self.session_cookie_name)
        self.ecourts_token = res.cookies.get(self.ecourts_token_cookie_name)
        if self.ecourts_token is None:
            raise ValueError(
                "Failed to get session token, not expected to happen. This could happen if the IP might have been detected as spam"
            )

    def get_cookie(self):
        return f"{self.ecourts_token_cookie_name}={self.ecourts_token}; {self.session_cookie_name}={self.session_id}"

    def update_session_id(self, response):
        new_session_cookie = response.cookies.get(self.session_cookie_name)
        if new_session_cookie:
            self.session_id = new_session_cookie

    def get_headers(self):
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": self.get_cookie(),
            "DNT": "1",
            "Origin": self.root_url,
            "Referer": self.root_url + "/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        }
        return headers

    def download(self):
        """Process a specific date range for this court"""
        if self.task.from_date is None or self.task.to_date is None:
            logger.info(f"No more data to download for: task: {self.task}")
            return

        search_payload = self.default_search_payload()
        search_payload["from_date"] = self.task.from_date
        search_payload["to_date"] = self.task.to_date
        self.init_user_session()
        search_payload["state_code"] = self.court_code
        search_payload["app_token"] = self.app_token
        results_available = True
        pdfs_downloaded = 0

        logger.info(f"Downloading data for: task: {self.task}")

        while results_available:
            try:
                response = self.request_api("POST", self.search_url, search_payload)
                res_dict = response.json()
                if self._results_exist_in_search_response(res_dict):
                    results = res_dict["reportrow"]["aaData"]
                    num_results = len(results)
                    
                    with tqdm(total=num_results, desc=f"Processing results for {self.task.court_code}", unit="result", leave=False) as result_pbar:
                        for idx, row in enumerate(results):
                            try:
                                is_pdf_downloaded = self.process_result_row(
                                    row, row_pos=idx
                                )
                                if is_pdf_downloaded:
                                    pdfs_downloaded += 1
                                    result_pbar.set_postfix(downloaded=pdfs_downloaded)
                                
                                result_pbar.update(1)
                                
                                if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                                    # after 25 downloads, need to solve captcha for every pdf link request
                                    logger.info(
                                        f"Downloaded {NO_CAPTCHA_BATCH_SIZE} pdfs, starting with fresh session, task: {self.task}"
                                    )
                                    break
                            except Exception as e:
                                logger.error(
                                    f"Error processing row {row}: {e}, task: {self.task}"
                                )
                                traceback.print_exc()
                                result_pbar.update(1)

                    if pdfs_downloaded >= NO_CAPTCHA_BATCH_SIZE:
                        pdfs_downloaded = 0
                        self.init_user_session()
                        search_payload["app_token"] = self.app_token
                        continue

                    # prepare next iteration
                    search_payload = self._prepare_next_iteration(search_payload)
                else:
                    # No more results for this date range
                    results_available = False

            except Exception as e:
                logger.error(f"Error processing task: {self.task}, {e}")
                traceback.print_exc()
                # results_available = False


class FileTrackingDownloader(Downloader):
    """Downloader that tracks downloaded files for S3 upload"""
    
    def __init__(self, task: CourtDateTask, downloaded_files: dict, force_pdf_download=False):
        super().__init__(task)
        self.downloaded_files = downloaded_files
        self.force_pdf_download = force_pdf_download
    
    def process_result_row(self, row, row_pos):
        """Override to track downloaded files"""
        html = row[1]
        soup = BeautifulSoup(html, "html.parser")
        
        if not (soup.button and "onclick" in soup.button.attrs):
            logger.info(
                f"No button found, likely multi language judgment, task: {self.task}"
            )
            with open("html-parse-failures.txt", "a") as f:
                f.write(html + "\n")
            return False
            
        pdf_fragment = self.extract_pdf_fragment(soup.button["onclick"])
        pdf_output_path = self.get_pdf_output_path(pdf_fragment)
        is_pdf_present = self.is_pdf_downloaded(pdf_fragment)
        
        # Force download PDFs even if they exist, or download if not present
        if self.force_pdf_download or not is_pdf_present:
            is_fresh_download = self.download_pdf(pdf_fragment, row_pos)
            if is_fresh_download:
                # Track the newly downloaded PDF
                self.downloaded_files['data'].append(str(pdf_output_path))
        else:
            is_fresh_download = False
            
        # If PDF exists (even if not newly downloaded), track it for tar creation
        if is_pdf_present or is_fresh_download:
            if str(pdf_output_path) not in self.downloaded_files['data']:
                self.downloaded_files['data'].append(str(pdf_output_path))
            
        # Always create/update metadata
        metadata_output = pdf_output_path.with_suffix(".json")
        metadata = {
            "court_code": self.court_code,
            "court_name": self.court_name,
            "raw_html": html,
            "pdf_link": pdf_fragment,
            "downloaded": is_pdf_present or is_fresh_download,
        }
        metadata_output.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_output, "w") as f:
            json.dump(metadata, f)
            
        # Track the metadata file
        self.downloaded_files['metadata'].append(str(metadata_output))
        
        return is_fresh_download


def upload_files_to_s3(court_code, downloaded_files):
    """Upload downloaded files to S3 bucket"""
    if not S3_AVAILABLE:
        print("[ERROR] S3 not available for upload")
        return
    
    if not downloaded_files['metadata'] and not downloaded_files['data']:
        print(f"No files to upload for court {court_code}")
        return
    
    s3_client = boto3.client('s3')
    current_time = datetime.now()
    year = current_time.year
    
    print(f"Starting S3 upload for court {court_code}")
    print(f"Files to upload: {len(downloaded_files['metadata'])} metadata, {len(downloaded_files['data'])} data files")
    
    # Extract bench from file paths and organize by bench
    bench_files = {}
    
    # Process metadata files
    for metadata_file in downloaded_files['metadata']:
        bench = extract_bench_from_path(metadata_file)
        if bench:
            if bench not in bench_files:
                bench_files[bench] = {'metadata': [], 'data': []}
            bench_files[bench]['metadata'].append(metadata_file)
    
    # Process data files  
    for data_file in downloaded_files['data']:
        bench = extract_bench_from_path(data_file)
        if bench:
            if bench not in bench_files:
                bench_files[bench] = {'metadata': [], 'data': []}
            bench_files[bench]['data'].append(data_file)
    
    # Upload files by bench
    for bench, files in bench_files.items():
        print(f"Uploading files for bench: {bench}")
        
        # Convert court code from 11~24 to 11_24 for S3 path
        s3_court_code = court_code.replace('~', '_')
        
        # Upload metadata files
        for metadata_file in files['metadata']:
            upload_single_file_to_s3(
                s3_client, metadata_file, s3_court_code, bench, year, 'metadata'
            )
        
        # Upload data files
        for data_file in files['data']:
            upload_single_file_to_s3(
                s3_client, data_file, s3_court_code, bench, year, 'data'
            )
        
        print(f"Completed upload for bench {bench}: {len(files['metadata'])} metadata, {len(files['data'])} data files")
        
        # ALSO create and upload tar files for this bench
        create_and_upload_tar_files(s3_client, s3_court_code, bench, year, files)
        
        # ALSO create and upload parquet files for this bench
        create_and_upload_parquet_files(s3_client, s3_court_code, bench, year, files)
    
    print(f"S3 upload completed for court {court_code}")


def create_and_upload_tar_files(s3_client, court_code, bench, year, files):
    """Download existing tar files, append new content, and upload back to S3"""
    import tarfile
    import tempfile
    
    print(f"  Creating/updating tar files for bench {bench}")
    
    # Handle metadata tar file
    if files['metadata']:
        metadata_tar_key = f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.tar.gz"
        
        # Try to download existing tar file
        existing_files_set = set()
        temp_existing_tar = None
        
        try:
            print(f"  Checking for existing metadata tar: {metadata_tar_key}")
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=metadata_tar_key)
            
            # Download existing tar to temp file
            temp_existing_tar = tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False)
            temp_existing_tar.write(response['Body'].read())
            temp_existing_tar.close()
            
            # Read existing files list to avoid duplicates
            with tarfile.open(temp_existing_tar.name, 'r:gz') as existing_tar:
                existing_files_set = set(existing_tar.getnames())
                print(f"  Found existing metadata tar with {len(existing_files_set)} files")
            
        except s3_client.exceptions.NoSuchKey:
            print(f"  No existing metadata tar found, will create new one")
        except Exception as e:
            print(f"  Warning: Could not download existing metadata tar: {e}")
        
        # Create new tar file with both existing and new content
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as temp_new_tar:
            with tarfile.open(temp_new_tar.name, 'w:gz') as new_tar:
                
                # First, add existing files if we have them
                if temp_existing_tar and os.path.exists(temp_existing_tar.name):
                    try:
                        with tarfile.open(temp_existing_tar.name, 'r:gz') as existing_tar:
                            for member in existing_tar.getmembers():
                                file_obj = existing_tar.extractfile(member)
                                if file_obj:
                                    new_tar.addfile(member, file_obj)
                    except Exception as e:
                        print(f"  Warning: Could not read existing tar content: {e}")
                
                # Then add new files (skip duplicates)
                new_files_added = 0
                for metadata_file in files['metadata']:
                    arcname = os.path.basename(metadata_file)
                    if arcname not in existing_files_set:
                        new_tar.add(metadata_file, arcname=arcname)
                        new_files_added += 1
                    else:
                        print(f"    Skipping duplicate: {arcname}")
                
                print(f"  Added {new_files_added} new metadata files to tar")
        
        # Upload updated tar to S3
        print(f"  Uploading updated metadata tar to s3://{S3_BUCKET}/{metadata_tar_key}")
        with open(temp_new_tar.name, 'rb') as f:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=metadata_tar_key,
                Body=f,
                ContentType='application/gzip'
            )
        
        # Clean up temp files
        os.unlink(temp_new_tar.name)
        if temp_existing_tar and os.path.exists(temp_existing_tar.name):
            os.unlink(temp_existing_tar.name)
        print(f"  ✅ Successfully uploaded updated metadata tar")
    
    # Handle data/PDF tar file
    if files['data']:
        data_tar_key = f"data/tar/year={year}/court={court_code}/bench={bench}/pdfs.tar"
        
        # Try to download existing tar file
        existing_files_set = set()
        temp_existing_tar = None
        
        try:
            print(f"  Checking for existing data tar: {data_tar_key}")
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=data_tar_key)
            
            # Download existing tar to temp file
            temp_existing_tar = tempfile.NamedTemporaryFile(suffix='.tar', delete=False)
            temp_existing_tar.write(response['Body'].read())
            temp_existing_tar.close()
            
            # Read existing files list to avoid duplicates
            with tarfile.open(temp_existing_tar.name, 'r') as existing_tar:
                existing_files_set = set(existing_tar.getnames())
                print(f"  Found existing data tar with {len(existing_files_set)} files")
            
        except s3_client.exceptions.NoSuchKey:
            print(f"  No existing data tar found, will create new one")
        except Exception as e:
            print(f"  Warning: Could not download existing data tar: {e}")
        
        # Create new tar file with both existing and new content
        with tempfile.NamedTemporaryFile(suffix='.tar', delete=False) as temp_new_tar:
            with tarfile.open(temp_new_tar.name, 'w') as new_tar:
                
                # First, add existing files if we have them
                if temp_existing_tar and os.path.exists(temp_existing_tar.name):
                    try:
                        with tarfile.open(temp_existing_tar.name, 'r') as existing_tar:
                            for member in existing_tar.getmembers():
                                file_obj = existing_tar.extractfile(member)
                                if file_obj:
                                    new_tar.addfile(member, file_obj)
                    except Exception as e:
                        print(f"  Warning: Could not read existing tar content: {e}")
                
                # Then add new files (skip duplicates)
                new_files_added = 0
                for data_file in files['data']:
                    arcname = os.path.basename(data_file)
                    if arcname not in existing_files_set:
                        new_tar.add(data_file, arcname=arcname)
                        new_files_added += 1
                    else:
                        print(f"    Skipping duplicate: {arcname}")
                
                print(f"  Added {new_files_added} new data files to tar")
        
        # Upload updated tar to S3
        print(f"  Uploading updated data tar to s3://{S3_BUCKET}/{data_tar_key}")
        with open(temp_new_tar.name, 'rb') as f:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=data_tar_key,
                Body=f,
                ContentType='application/x-tar'
            )
        
        # Clean up temp files
        os.unlink(temp_new_tar.name)
        if temp_existing_tar and os.path.exists(temp_existing_tar.name):
            os.unlink(temp_existing_tar.name)
        print(f"  ✅ Successfully uploaded updated data tar")


def create_and_upload_zip_files(s3_client, court_code, bench, year, files):
    """Download existing zip files, append new content, and upload back to S3"""
    import zipfile
    import tempfile
    
    print(f"  Creating/updating zip files for bench {bench}")
    
    # Handle metadata zip file for JSON files only
    if files['metadata']:
        metadata_zip_key = f"metadata/zip/year={year}/court={court_code}/bench={bench}/metadata.zip"
        
        # Try to download existing zip file
        existing_files_set = set()
        temp_existing_zip = None
        
        try:
            print(f"  Checking for existing metadata zip: {metadata_zip_key}")
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=metadata_zip_key)
            
            # Download existing zip to temp file
            temp_existing_zip = tempfile.NamedTemporaryFile(suffix='.zip', delete=False)
            temp_existing_zip.write(response['Body'].read())
            temp_existing_zip.close()
            
            # Read existing files list to avoid duplicates
            with zipfile.ZipFile(temp_existing_zip.name, 'r') as existing_zip:
                existing_files_set = set(existing_zip.namelist())
                print(f"  Found existing metadata zip with {len(existing_files_set)} files")
            
        except s3_client.exceptions.NoSuchKey:
            print(f"  No existing metadata zip found, will create new one")
        except Exception as e:
            print(f"  Warning: Could not download existing metadata zip: {e}")
        
        # Create new zip file with both existing and new content
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_new_zip:
            with zipfile.ZipFile(temp_new_zip.name, 'w', zipfile.ZIP_DEFLATED) as new_zip:
                
                # First, add existing files if we have them
                if temp_existing_zip and os.path.exists(temp_existing_zip.name):
                    try:
                        with zipfile.ZipFile(temp_existing_zip.name, 'r') as existing_zip:
                            for file_info in existing_zip.infolist():
                                file_data = existing_zip.read(file_info.filename)
                                new_zip.writestr(file_info, file_data)
                    except Exception as e:
                        print(f"  Warning: Could not read existing zip content: {e}")
                
                # Then add new files (skip duplicates)
                new_files_added = 0
                for metadata_file in files['metadata']:
                    arcname = os.path.basename(metadata_file)
                    if arcname not in existing_files_set:
                        new_zip.write(metadata_file, arcname=arcname)
                        new_files_added += 1
                    else:
                        print(f"    Skipping duplicate: {arcname}")
                
                print(f"  Added {new_files_added} new metadata files to zip")
        
        # Upload updated zip to S3
        print(f"  Uploading updated metadata zip to s3://{S3_BUCKET}/{metadata_zip_key}")
        with open(temp_new_zip.name, 'rb') as f:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=metadata_zip_key,
                Body=f,
                ContentType='application/zip'
            )
        
        # Clean up temp files
        os.unlink(temp_new_zip.name)
        if temp_existing_zip and os.path.exists(temp_existing_zip.name):
            os.unlink(temp_existing_zip.name)
        print(f"  ✅ Successfully uploaded updated metadata zip")


def create_and_upload_parquet_files(s3_client, court_code, bench, year, files):
    """Create parquet files from JSON metadata and upload to S3"""
    if not PARQUET_AVAILABLE:
        print("  ❌ Parquet libraries not available, skipping parquet creation")
        print("  Install with: pip install pyarrow")
        return
        
    # Only process metadata files (JSON) for parquet conversion
    if not files['metadata']:
        print("  No metadata files to convert to parquet")
        return
    
    try:
        import tempfile
        from pathlib import Path
        
        print(f"  Creating parquet files for bench {bench}")
        
        # Create temporary directory structure for JSON files
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Copy JSON files to temporary directory
            for json_file in files['metadata']:
                json_path = Path(json_file)
                dest_path = temp_path / json_path.name
                shutil.copy2(json_file, dest_path)
            
            # Create parquet file using MetadataProcessor
            parquet_file = temp_path / "metadata.parquet"
            
            mp = MetadataProcessor(temp_path, output_path=parquet_file)
            mp.process()
            print(f"  Successfully created parquet file with {len(files['metadata'])} JSON files")
            
            # Upload parquet file to S3
            parquet_key = f"metadata/parquet/year={year}/court={court_code}/bench={bench}/metadata.parquet"
            print(f"  Uploading parquet to s3://{S3_BUCKET}/{parquet_key}")
            
            with open(parquet_file, 'rb') as f:
                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=parquet_key,
                    Body=f,
                    ContentType='application/octet-stream'
                )
            
            print("  ✅ Successfully uploaded parquet file")
            
    except Exception as e:
        print(f"  ❌ Failed to create parquet file: {e}")
        import traceback
        traceback.print_exc()


def extract_bench_from_path(file_path):
    """Extract bench name from file path like data/court/cnrorders/sikkimhc_pg/orders/..."""
    import os
    path_parts = file_path.split(os.sep)
    try:
        # Find the index of 'cnrorders' and get the next part as bench
        cnrorders_index = path_parts.index('cnrorders')
        if cnrorders_index + 1 < len(path_parts):
            return path_parts[cnrorders_index + 1]
    except (ValueError, IndexError):
        pass
    return None


def upload_single_file_to_s3(s3_client, local_file_path, court_code, bench, year, file_type):
    """Upload a single file to S3"""
    try:
        # Determine S3 key based on file type and your bucket structure
        filename = os.path.basename(local_file_path)
        
        if file_type == 'metadata':
            # Upload to metadata/json/year=2025/court=11_24/bench=sikkimhc_pg/
            s3_key = f"metadata/json/year={year}/court={court_code}/bench={bench}/{filename}"
        else:  # data/pdf files
            # Upload to data/pdf/year=2025/court=11_24/bench=sikkimhc_pg/
            s3_key = f"data/pdf/year={year}/court={court_code}/bench={bench}/{filename}"
        
        # Upload the file
        print(f"  Uploading {filename} to s3://{S3_BUCKET}/{s3_key}")
        
        with open(local_file_path, 'rb') as f:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=f,
                ContentType='application/json' if filename.endswith('.json') else 'application/pdf'
            )
        
        print(f"  ✅ Successfully uploaded {filename}")
        return True
        
    except Exception as e:
        print(f"  ❌ Failed to upload {local_file_path}: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--court_code", type=str, default=None)
    parser.add_argument("--court_codes", type=str, default=[])
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--day_step", type=int, default=1, help="Number of days per chunk"
    )
    parser.add_argument("--max_workers", type=int, default=2, help="Number of workers")
    parser.add_argument("--sync-s3", action="store_true", help="Run S3 sync to download incremental data and sync to S3")
    parser.add_argument("--fetch-dates", action="store_true", help="Fetch latest dates from tar index files")
    parser.add_argument("--test", action="store_true", help="Test mode: download only 1 day for each court (use with --sync-s3)")
    args = parser.parse_args()

    # Handle different modes
    if args.fetch_dates:
        court_dates = get_court_dates_from_index_files()
        print(f"Found dates for {len(court_dates)} courts")
    elif args.sync_s3:
        sync_to_s3(test_mode=args.test)
    else:
        # Regular download mode
        if args.court_codes:
            assert (
                args.court_code is None
            ), "court_code and court_codes cannot both be provided"
            court_codes = args.court_codes.split(",")
        elif args.court_code:
            court_codes = [args.court_code]
        else:
            court_codes = None

        run(
            court_codes, args.start_date, args.end_date, args.day_step, args.max_workers
        )

"""
captcha prompt while downloading pdf seems to be different from session timeout
Every search API request returns a new app_token in response payload and new PHPSESSID in response cookies that need to be sent in the next request.
openpdfcaptcha request refreshes the app_token but not PHPSESSID

"""