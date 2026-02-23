import os
import re
import concurrent.futures
from tqdm import tqdm
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
INPUT_DIR = '10k_filings'       # Raw downloaded files
OUTPUT_DIR = 'cleaned_filings'  # Clean output

def clean_file_strictly(raw_text):
    """
    Strict implementation of Loughran & McDonald 'Internet Appendix'
    cleaning logic, adapted for modern Inline XBRL 10-Ks.
    """
    
    # Remove SEC Headers and text between <SEC-HEADER> tags
    text = re.sub(r'<SEC-HEADER>.*?</SEC-HEADER>', '', raw_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<IMS-HEADER>.*?</IMS-HEADER>', '', text, flags=re.DOTALL | re.IGNORECASE)

    # DESTROY INLINE XBRL METADATA
    text = re.sub(r'<ix:header[^>]*>.*?</ix:header>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<ix:hidden[^>]*>.*?</ix:hidden>', '', text, flags=re.DOTALL | re.IGNORECASE)

    # Remove all text appearing within <TYPE>EX tags
    # STRATEGY: We split the file into documents. We KEEP the 10-K and DROP the exhibits.
    kept_documents = []
    # Split by <DOCUMENT> tags
    docs = re.split(r'<DOCUMENT>', text, flags=re.IGNORECASE)
    
    for doc in docs:
        if not doc.strip(): continue
        
        # Check <TYPE>
        type_match = re.search(r'<TYPE>(.*?)\n', doc, re.IGNORECASE)
        if type_match:
            doc_type = type_match.group(1).strip().upper()
            
            # Drop Exhibits (EX-), Graphics, ZIPs, XML, Excel (Junk)
            if (doc_type.startswith('EX') or 
                doc_type.startswith('XML') or 
                doc_type.startswith('GRAPHIC') or 
                doc_type.startswith('ZIP') or 
                doc_type.startswith('EXCEL') or
                doc_type.startswith('JSON')):
                continue # Skip this junk document
        
        # Keep the cleaned part
        kept_documents.append(doc)
        
    # Reassemble the kept parts
    text = '\n'.join(kept_documents)

    # BeautifulSoup Cleaning (Tables & HTML)
    try:
        soup = BeautifulSoup(text, 'lxml')
    except:
        soup = BeautifulSoup(text, 'html.parser')

    # Remove Script/Style
    for tag in soup(['script', 'style']):
        tag.decompose()

    # Remove all text appearing within <TABLE> tags where table > 25% Numbers
    for table in soup.find_all('table'):
        table_text = table.get_text(separator=' ', strip=True)
        # Calculate density
        clean_chars = re.sub(r'\s', '', table_text)
        if len(clean_chars) == 0:
            table.decompose()
            continue
            
        num_digits = sum(c.isdigit() for c in clean_chars)
        density = num_digits / len(clean_chars)
        
        if density > 0.25: # 25% Rule
            table.decompose() # Delete Financial Table
        else:
            table.unwrap() # Keep Text Table (remove tags, keep text)

    # Get Text
    clean_text = soup.get_text(separator=' ')

    # Remove Binary/Graphics Artifacts
    # Strategy: Real words are rarely > 50 chars. Encoded graphics are huge blocks of text.
    clean_text = re.sub(r'\S{50,}', ' ', clean_text)

    # Re-encode and Whitespace
    clean_text = clean_text.replace('\xa0', ' ') # distinct from &nbsp; in some parsers
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    
    # Convert to uppercase (LM dictionaries are uppercase)
    clean_text = clean_text.upper()
    
    # Remove all non-alphabetic characters (punctuation, numbers, special symbols)
    # This leaves only pure words separated by spaces
    clean_text = re.sub(r'[^A-Z\s]', ' ', clean_text)
    
    # Collapse multiple spaces into a single space again
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    # ---------------------------------------
    
    return clean_text

def process_single_file(filename):
    """Worker function to process a single file for multiprocessing."""
    in_path = os.path.join(INPUT_DIR, filename)
    out_path = os.path.join(OUTPUT_DIR, filename)
    
    # Skip if already processed (allows resuming if interrupted)
    if os.path.exists(out_path):
        return True
        
    try:
        with open(in_path, 'r', encoding='utf-8', errors='ignore') as f:
            raw_content = f.read()
        
        # Skip empty raw files
        if len(raw_content) < 1000:
            # If raw file is tiny, it's a download error
            return False

        cleaned_content = clean_file_strictly(raw_content)
        
        # If result is empty (< 1KB), something went wrong, but save it anyway
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write(cleaned_content)
            
        return True
    except Exception as e:
        return f"Error cleaning {filename}: {e}"

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.txt')]
    print(f"Cleaning {len(files)} files utilizing Multiprocessing...")
    
    success_count = 0
    errors = []
    
    # Use all available CPU cores minus 1 to keep your computer responsive
    max_workers = max(1, os.cpu_count() - 1)
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Wrap with tqdm for a progress bar
        results = list(tqdm(executor.map(process_single_file, files), total=len(files), desc="Processing"))
        
    for res in results:
        if res is True:
            success_count += 1
        elif isinstance(res, str): # Caught an error message
            errors.append(res)

    print(f"\nCleaning Complete.")
    print(f"Processed {success_count} files successfully.")
    if errors:
        print(f"Encountered {len(errors)} errors. Check data.")
    print(f"Files saved to: {os.path.abspath(OUTPUT_DIR)}")

if __name__ == "__main__":
    main()