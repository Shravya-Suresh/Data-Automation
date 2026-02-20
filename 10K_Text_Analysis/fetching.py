import requests
import pandas as pd
import time
import tqdm 
import os
from dotenv import load_dotenv

load_dotenv()
# Use the same variable name we set up in your .env
CONTACT_EMAIL = os.getenv('SEC_EMAIL', 'researcher@domain.com')
HEADERS = {'User-Agent': f'DigitalMaturityProject {CONTACT_EMAIL}'}

INPUT_FILE = 'Target_List_MultiYear.csv' # Target CIK list
OUTPUT_DIR = '10k_filings' # Output folder with all companies txt files

def download_filings():
    # Setup Directory
    if os.path.basename(os.getcwd()) == OUTPUT_DIR:
        save_dir = "."
    else:
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
        save_dir = OUTPUT_DIR
    
    print(f"Saving files to: {os.path.abspath(save_dir)}")
    
    # Load the Target List
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: Could not find {INPUT_FILE} in {os.getcwd()}")
        return

    print(f"Found {len(df)} filings to download.")
    
    # Download Loop
    success_count = 0
    error_count = 0
    
    # Using tqdm.tqdm() explicitly to satisfy the TypeError
    for index, row in tqdm.tqdm(df.iterrows(), total=len(df), desc="Downloading"):
        ticker = row['ticker']
        # Sanitize date to ensure valid filename
        date_filed = str(row['Date Filed']).split(' ')[0] 
        url = row['10k_url']
        
        # Create filename: TICKER_YYYY-MM-DD.txt
        filename = f"{ticker}_{date_filed}.txt"
        filepath = os.path.join(save_dir, filename)
        
        # Skip if already exists
        if os.path.exists(filepath):
            continue
            
        try:
            r = requests.get(url, headers=HEADERS)
            
            # Handle Rate Limiting
            if r.status_code in [403, 429]:
                tqdm.tqdm.write(f"Rate limit hit! Pausing for 10 seconds...")
                time.sleep(10)
                r = requests.get(url, headers=HEADERS)
            
            r.raise_for_status() 
            
            with open(filepath, 'wb') as f:
                f.write(r.content)
                
            success_count += 1
            time.sleep(0.15) # Sleep to respect SEC limits
            
        except Exception as e:
            # use tqdm.write so it doesn't break the progress bar layout
            tqdm.tqdm.write(f"Error downloading {ticker}: {e}")
            error_count += 1
            
    print(f"\nPhase 2 Complete.")
    print(f"Successfully downloaded: {success_count}")
    print(f"Errors: {error_count}")

if __name__ == "__main__":
    download_filings()