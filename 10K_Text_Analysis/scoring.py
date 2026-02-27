import os
import pandas as pd
import re
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

# --- CONFIGURATION ---
INPUT_DIR = 'cleaned_filings' 
OUTPUT_FILE = 'six_pillars_data.xlsx' 
MAPPING_FILE = 'Target_List_MultiYear.csv' 

# Original Dictionary
KEYWORDS = {
    'DIMENSION_1_PURPOSE': [
        'purpose', 'mission', 'social impact', 'shared value', 'stakeholder capitalism',
        'sustainability', 'esg', 'carbon footprint', 'net zero', 'circular economy',
        'social responsibility', 'community impact', 'inclusive', 'accessible', 'ethical ai',
        'transparency', 'accountability', 'responsible innovation', 'trust',
        'b corp', 'benefit corporation', 'triple bottom line',
        'digital inclusion', 'digital divide', 'accessibility standards'
    ],
    'DIMENSION_2_PHYGITAL': [
        'omnichannel', 'click-and-collect', 'in-store digital', 'iot', 
        'augmented reality', 'seamless experience', 'buy online pick up in store', 
        'connected stores', 'smart retail', 'hybrid experience'
    ],
    'DIMENSION_3_PLATFORMS': [
        'platform', 'marketplace', 'ecosystem', 'api', 'developer community',
        'network effect', 'open source', 'partner ecosystem', 'app store', 
        'third-party developers', 'platform economy', 'api-first'
    ],
    'DIMENSION_4_PARTICIPATION': [
        'co-creation', 'crowdsourcing', 'user-generated content', 'community-led',
        'open innovation', 'customer feedback loops', 'beta testing', 'user forums',
        'brand ambassadors', 'maker community', 'hackathons'
    ],
    'DIMENSION_5_PARTNERSHIPS': [
        'strategic alliance', 'joint venture', 'collaboration', 'partner network',
        'cross-industry collaboration', 'innovation ecosystem', 'open innovation',
        'consortium', 'university partnership', 'startup accelerator', 'corporate venture capital'
    ],
    'DIMENSION_6_PEOPLE': [
        'digital skills', 'upskilling', 'reskilling', 'agile', 'digital culture',
        'remote work', 'hybrid work', 'employee experience', 'digital talent',
        'diversity and inclusion', 'growth mindset', 'continuous learning'
    ]
}

# PRE-CLEAN DICTIONARY
# Formats the keywords to perfectly match the Phase 3 cleaned 10-K text
CLEANED_KEYWORDS = {}
for dim, terms in KEYWORDS.items():
    clean_terms = []
    for term in terms:
        # Convert to uppercase and replace non-alpha with space (removes hyphens)
        c_term = re.sub(r'[^A-Z\s]', ' ', term.upper())
        # Collapse multiple spaces
        c_term = re.sub(r'\s+', ' ', c_term).strip()
        clean_terms.append(c_term)
    CLEANED_KEYWORDS[dim] = clean_terms

# PRE-COMPILE REGEX
COMPILED_PATTERNS = {}
for dim, terms in CLEANED_KEYWORDS.items():
    escaped_terms = [re.escape(term) for term in terms]
    # Use word boundaries. No IGNORECASE needed since everything is uppercase.
    pattern_str = r'\b(' + '|'.join(escaped_terms) + r')\b'
    COMPILED_PATTERNS[dim] = re.compile(pattern_str)

def process_single_file(filename):
    """Worker function to count raw matches and term frequencies."""
    try:
        basename = filename.replace('.txt', '')
        parts = basename.split('_')
        ticker = parts[0]
        date_filed = parts[1]
        
        path = os.path.join(INPUT_DIR, filename)
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        words = content.split()
        total_words = len(words)
        
        scores = {
            'Ticker': ticker,
            'Date_Filed': date_filed,
            'Filename': filename,
            'Total_Word_Count': total_words
        }
        
        for dim, pattern in COMPILED_PATTERNS.items():
            matches = pattern.findall(content)
            count = len(matches)
            # Store raw count
            scores[dim + '_raw'] = count
            # Store Term Frequency (Count / Total Words) for later normalization
            scores[dim + '_freq'] = count / total_words if total_words > 0 else 0
            
        return scores
        
    except Exception as e:
        return None

def main():
    if not os.path.exists(INPUT_DIR):
        print(f"Error: {INPUT_DIR} not found.")
        return

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.txt')]
    num_cores = cpu_count()
    print(f"Scoring {len(files)} files using {num_cores} cores...")
    
    with Pool(processes=num_cores) as pool:
        results = list(tqdm(pool.imap_unordered(process_single_file, files), total=len(files)))
    
    # Filter out any failed files
    results = [r for r in results if r]
    df_results = pd.DataFrame(results)
    
    # ADD COMPANY NAMES
    try:
        if os.path.exists(MAPPING_FILE):
            df_map = pd.read_csv(MAPPING_FILE)
            if 'ticker' in df_map.columns and 'universal_name' in df_map.columns:
                ticker_map = df_map.drop_duplicates('ticker').set_index('ticker')['universal_name'].to_dict()
                df_results['Company_Name'] = df_results['Ticker'].map(ticker_map)
            else:
                df_results['Company_Name'] = df_results['Ticker']
    except Exception as e:
        print(f"Name mapping failed: {e}")
        df_results['Company_Name'] = df_results['Ticker']

    # NORMALIZATION (0-100 SCALE)
    print("Applying Min-Max Normalization to generate 0-100 scores...")
    for dim in CLEANED_KEYWORDS.keys():
        min_val = df_results[dim + '_freq'].min()
        max_val = df_results[dim + '_freq'].max()
        
        if max_val > min_val:
            # Scale from 0 to 100 based on the highest/lowest frequency in the dataset
            df_results[dim + '_Score_0_to_100'] = ((df_results[dim + '_freq'] - min_val) / (max_val - min_val)) * 100
        else:
            df_results[dim + '_Score_0_to_100'] = 0.0
            
        # Round to 2 decimal places for a clean spreadsheet
        df_results[dim + '_Score_0_to_100'] = df_results[dim + '_Score_0_to_100'].round(2)

    # ORGANIZE FINAL CSV -> XLSX
    # Drop the intermediate '_freq' columns to keep the dataset clean
    drop_cols = [c for c in df_results.columns if '_freq' in c]
    df_results = df_results.drop(columns=drop_cols)
    
    # Order columns logically
    front_cols = ['Company_Name', 'Ticker', 'Date_Filed', 'Total_Word_Count']
    score_cols = [c for c in df_results.columns if '0_to_100' in c]
    raw_cols = [c for c in df_results.columns if '_raw' in c]
    
    final_cols = front_cols + score_cols + raw_cols
    df_results = df_results[[c for c in final_cols if c in df_results.columns]]
    
    # Sort chronologically by company
    df_results = df_results.sort_values(['Company_Name', 'Date_Filed'])
    
    # Save formatted dataset to Excel
    df_results.to_excel(OUTPUT_FILE, index=False)
    
    print(f"\nPhase 4 Complete.")
    print(f"Dataset securely saved to: {os.path.abspath(OUTPUT_FILE)}")

if __name__ == "__main__":
    main()