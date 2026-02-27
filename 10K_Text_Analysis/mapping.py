"""
Phase 1: SEC EDGAR Target Mapping

This script reads a master list of target companies, maps their ticker 
symbols to SEC Central Index Keys (CIKs), and crawls the SEC Master Index 
to generate exact download URLs for their 10-K filings across target years.
"""

import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

# Load the variables from your hidden .env file
load_dotenv()

# Fetch the email securely (defaults to a generic string if .env is missing)
CONTACT_EMAIL = os.getenv('SEC_EMAIL', 'researcher@domain.com')

# --- CONFIGURATION ---
# The User-Agent is now constructed dynamically
HEADERS = {'User-Agent': f'DigitalMaturityProject {CONTACT_EMAIL}'}
# Define relative paths (Assuming data is kept in the local directory)
INPUT_FILE = 'company_tickers_and_names.csv'  # List of target companies
OUTPUT_FILE = 'Target_List_MultiYear.csv'  # Final mapped list with URLs

# Define the temporal scope of the research
TARGET_YEARS = [2023, 2024, 2025, 2026]

def get_sec_tickers_map():
    """
    Fetches the official SEC ticker-to-CIK mapping JSON.
    Returns a DataFrame containing tickers and corresponding CIKs.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    print(f"Fetching CIK map from {url}...")
    try:
        r = requests.get(url, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame.from_dict(data, orient='index')
        df['ticker'] = df['ticker'].str.upper()
        return df
    except Exception as e:
        print(f"Error fetching ticker map: {e}")
        return pd.DataFrame()

def get_master_index(year, quarter):
    """
    Fetches and parses the SEC Master Index for a specific year and quarter.
    Filters the raw text file to extract CIK, Company Name, Form Type, Date, and File Path.
    """
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    print(f"  Fetching Index: {year} Q{quarter}...", end=" ")
    
    try:
        r = requests.get(url, headers=HEADERS)
        if r.status_code == 403:
            print("BLOCKED by SEC. Check User-Agent format.")
            return pd.DataFrame()
        if r.status_code == 404:
            print("Not found (Quarter has not occurred yet).")
            return pd.DataFrame()
        r.raise_for_status()
        
        # The SEC index is latin-1 encoded
        content = r.content.decode('latin-1')
        lines = content.splitlines()
        
        # Locate the start of the actual data table
        data_start = 0
        for i, line in enumerate(lines):
            if "CIK" in line and "Company Name" in line and "Form Type" in line:
                data_start = i + 2 
                break
        
        if data_start == 0:
            return pd.DataFrame()
            
        # Parse the pipe-delimited records
        records = [line.split('|') for line in lines[data_start:] if line.strip()]
        df = pd.DataFrame(records, columns=['CIK', 'Company Name', 'Form Type', 'Date Filed', 'Filename'])
        print(f"Found {len(df)} total filings.")
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def main():
    # Load cleaned Fortune 500 list
    print(f"Loading input file: {INPUT_FILE}...")
    f500_df = pd.read_csv(INPUT_FILE)
    
    # Rename the CSV columns to match what the script expects internally
    f500_df = f500_df.rename(columns={'COMPANY NAME': 'company', 'TICKER': 'ticker'})

    # Create a "dummy" ticker for private/mutual companies without one by removing spaces from their name
    # Example: "State Farm Insurance" becomes "STATEFARMINSURANCE"
    f500_df['ticker'] = f500_df['ticker'].fillna(f500_df['company'].astype(str).str.replace(' ', '').str.upper())
    
    # Make a copy as valid companies
    valid_companies = f500_df.copy()
    
    # Ensure tickers are uppercase and clean
    valid_companies['ticker'] = valid_companies['ticker'].str.upper().str.strip()
    
    # Rename 'company' to 'universal_name' for consistency
    valid_companies = valid_companies.rename(columns={'company': 'universal_name'})

    print(f"Loaded {len(valid_companies)} public & private companies to map.")

    # Get SEC map
    sec_map = get_sec_tickers_map()

    # Format CIKs to 10-digit strings (Preserves leading zeros for SEC matching)
    if not sec_map.empty:
        sec_map['CIK_str'] = sec_map['cik_str'].astype(str).str.zfill(10)
        ticker_to_cik = dict(zip(sec_map['ticker'], sec_map['CIK_str']))
    else:
        ticker_to_cik = {}

    # Force-map missing companies that are private, mutuals, cooperatives, or SEC dropouts.
    overrides = {
        'AMRK': '0001591588', # A-Mark Precious Metals
        'ATUS': '0001702780', # Altice USA
        'BERY': '0001378992', # Berry Global Group
        'BRKA': '0001067983', # Berkshire Hathaway 
        'DFS':  '0001393612', # Discover Financial Services
        'EDR':  '0001766363', # Endeavor Group Holdings
        'FI':   '0000798354', # Fiserv
        'FL':   '0000850209', # Foot Locker
        'HES':  '0000004447', # Hess
        'IPG':  '0000051644', # Interpublic Group
        'K':    '0000055067', # Kellanova (formerly Kellogg)
        'MMC':  '0000062709', # Marsh & McLennan
        'OMI':  '0000075252', # Owens & Minor
        'PARA': '0000813828', # Paramount Global
        'SKX':  '0001065696', # Skechers U.S.A.
        'SPTN': '0000877422', # SpartanNash
        'SQ':   '0001512673', # Block
        'WBA':  '0001618921', # Walgreens Boots Alliance
        'X':    '0001163302', # United States Steel
    }
    
    # Inject the missing companies into the dictionary
    ticker_to_cik.update(overrides)
    
    # Use .map() to assign CIKs directly to your original rows
    valid_companies['CIK'] = valid_companies['ticker'].map(ticker_to_cik)
    
    # Drop rows that still didn't find a CIK (if any remain)
    unmapped = valid_companies[valid_companies['CIK'].isna()]
    if not unmapped.empty:
        print(f"Warning: Could not map {len(unmapped)} companies (e.g., {unmapped['universal_name'].head(3).tolist()})")
        
    valid_companies = valid_companies.dropna(subset=['CIK']).copy()
    
    target_ciks = set(valid_companies['CIK'].unique())
    print(f"Successfully mapped {len(target_ciks)} unique CIKs.")

    # Locate 10-K URLs across target years
    all_target_filings = []
    
    for year in TARGET_YEARS:
        print(f"\nProcessing Year {year}...")
        for q in [1, 2, 3, 4]:
            idx = get_master_index(year, q)
            if idx.empty: 
                continue
            
           # Filter strictly for 10-K forms
            k_filings = idx[idx['Form Type'] == '10-K'].copy()
            
            # Standardize Index CIKs to 10-digit strings to match our target_ciks
            k_filings['CIK'] = k_filings['CIK'].astype(str).str.strip().str.zfill(10)
            relevant_filings = k_filings[k_filings['CIK'].isin(target_ciks)].copy()
            
            if not relevant_filings.empty:
                all_target_filings.append(relevant_filings)
                
            # Respect SEC rate limits (Max 10 requests per second)
            time.sleep(0.1) 

    if not all_target_filings:
        print("No target 10-K filings found in the specified years.")
        return

    # 4. Compile and Format Final Output
    total_filings = pd.concat(all_target_filings, ignore_index=True)
    
    # Merge back to retain original ticker and company name
    final_output = total_filings.merge(valid_companies[['CIK', 'ticker', 'universal_name']], on='CIK', how='left')
    
    # Construct the full SEC download URL
    final_output['10k_url'] = "https://www.sec.gov/Archives/" + final_output['Filename']
    
    # Sort chronologically by company
    final_output = final_output.sort_values(['ticker', 'Date Filed'])
    
    # Save formatted dataset
    cols = ['universal_name', 'ticker', 'CIK', 'Date Filed', '10k_url']
    final_output[cols].to_csv(OUTPUT_FILE, index=False)
    
    print(f"\nPhase 1 Complete.")
    print(f"Discovered {len(final_output)} total 10-K filings across {TARGET_YEARS}.")
    print(f"Dataset securely saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()