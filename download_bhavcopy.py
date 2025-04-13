"""
Script to download NSE Bhavcopy files for the past 5 years
"""

import os
import requests
import datetime
import time
import calendar
import json
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
import zipfile
import io

# Create directories for storing bhavcopy files
bhavcopy_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bhavcopy')
os.makedirs(bhavcopy_dir, exist_ok=True)
raw_dir = os.path.join(bhavcopy_dir, 'raw')
os.makedirs(raw_dir, exist_ok=True)
extracted_dir = os.path.join(bhavcopy_dir, 'extracted')
os.makedirs(extracted_dir, exist_ok=True)

# Calculate date range for past 5 years
end_date = datetime.datetime.now()
start_date = end_date - relativedelta(years=5)
# start_date = end_date - relativedelta(months=12)

# Format for NSE Bhavcopy URLs
# Example: https://archives.nseindia.com/content/historical/DERIVATIVES/2020/APR/fo13APR2020bhav.csv.zip

def get_bhavcopy_url(date):
    """Generate URL for NSE Bhavcopy file for a given date"""
    year = date.strftime("%Y")
    month = date.strftime("%b").upper()
    day = date.strftime("%d")
    formatted_date = f"{day}{month}{year}"
    url = f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month}/fo{formatted_date}bhav.csv.zip"
    return url, formatted_date

def download_bhavcopy(date):
    """Download NSE Bhavcopy file for a given date"""
    url, formatted_date = get_bhavcopy_url(date)
    zip_file_path = os.path.join(raw_dir, f"fo{formatted_date}bhav.csv.zip")
    csv_file_path = os.path.join(extracted_dir, f"fo{formatted_date}bhav.csv")
    
    #  TIME DELAY -------------------
    time.sleep(1) 
    
    # Skip if already downloaded and extracted
    if os.path.exists(csv_file_path):
        print(f"Already downloaded and extracted: {formatted_date}")
        return True
    
    try:
        # Add headers to mimic browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            # Save zip file
            with open(zip_file_path, 'wb') as f:
                f.write(response.content)
            
            # Extract zip file
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(extracted_dir)
            
            print(f"Successfully downloaded and extracted: {formatted_date}")
            return True
        else:
            # Weekend or holiday, no data available
            print(f"No data available for: {formatted_date} (Status code: {response.status_code})")
            return False
    except Exception as e:
        print(f"Error downloading {formatted_date}: {str(e)}")
        return False

def download_bhavcopies_for_date_range(start_date, end_date, max_workers=1):
    """Download NSE Bhavcopy files for a date range using multiple threads"""
    # Generate list of dates (excluding weekends)
    dates = []
    current_date = start_date
    while current_date <= end_date:
        # Skip weekends (5: Saturday, 6: Sunday)
        if current_date.weekday() < 5:
            dates.append(current_date)
        current_date += datetime.timedelta(days=1)
    
    print(f"Downloading Bhavcopy files for {len(dates)} trading days...")
    
    # Use ThreadPoolExecutor for parallel downloads
    successful_downloads = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(download_bhavcopy, dates))
        successful_downloads = sum(1 for result in results if result)
    
    print(f"Successfully downloaded {successful_downloads} out of {len(dates)} Bhavcopy files")
    return successful_downloads

if __name__ == "__main__":
    print(f"Downloading NSE Bhavcopy files from {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")
    
    # Load F&O scripts
    with open('nse_fno_scripts.json', 'r') as f:
        fno_scripts = json.load(f)
    
    print(f"Found {len(fno_scripts['index_futures'])} index futures and {len(fno_scripts['individual_securities'])} individual securities")
    
    # Download Bhavcopy files
    download_bhavcopies_for_date_range(start_date, end_date)
