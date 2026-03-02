import pandas as pd
import numpy as np

# Define paths
scores_file = 'average_pillars_scores.xlsx' 
fortune_file = '2025.Fortune.500.financials.xlsx'
edgar_file = 'edgar_results_2026-02-23.xlsx'

# Read data
print("Loading Scores and Fortune 500 data...")
df_scores = pd.read_excel(scores_file)  
df_fortune = pd.read_excel(fortune_file) 

# Drop 'Unnamed' and 'FN:' columns
cols_to_drop = [col for col in df_fortune.columns if 'Unnamed' in str(col) or 'FN:' in str(col)]
df_fortune = df_fortune.drop(columns=cols_to_drop)

# The Fortune 500 file has 500 companies plus a few empty/footnote rows at the bottom.
# This drops any row that doesn't have a Company Name, leaving exactly 500 clean rows.
df_fortune = df_fortune.dropna(subset=['COMPANY NAME'])

print("Loading EDGAR data...")
# Load the summary sheet
df_edgar_summary = pd.read_excel(edgar_file, sheet_name='Summary by Company')

# Load the first sheet to extract the company_name -> ticker mapping
print("Extracting Tickers from EDGAR Sheet 1...")
df_edgar_sheet1 = pd.read_excel(edgar_file, sheet_name=0, usecols=['company_name', 'ticker'])
ticker_mapping = df_edgar_sheet1.drop_duplicates(subset=['company_name'])

# Add ticker to summary sheet
df_edgar_summary.columns = [col.lower().replace(' ', '_') for col in df_edgar_summary.columns]
df_edgar = pd.merge(df_edgar_summary, ticker_mapping, on='company_name', how='left')

# Standardize ticker
df_fortune = df_fortune.rename(columns={'TICKER': 'Ticker'})
df_edgar = df_edgar.rename(columns={'ticker': 'Ticker'})

df_scores['Ticker'] = df_scores['Ticker'].astype(str).str.upper().str.strip()
df_fortune['Ticker'] = df_fortune['Ticker'].astype(str).str.upper().str.strip()
df_edgar['Ticker'] = df_edgar['Ticker'].astype(str).str.upper().str.strip()

# Replace 'NAN' or 'NONE' strings with actual empty missing values
df_fortune['Ticker'] = df_fortune['Ticker'].replace(['NAN', 'NONE', ''], np.nan)
df_scores['Ticker'] = df_scores['Ticker'].replace(['NAN', 'NONE', ''], np.nan)
df_edgar['Ticker'] = df_edgar['Ticker'].replace(['NAN', 'NONE', ''], np.nan)

# For private companies with no ticker, use their UPPERCASE COMPANY NAME as a temporary bridge!
df_fortune['Ticker'] = df_fortune['Ticker'].fillna(df_fortune['COMPANY NAME'].astype(str).str.upper().str.strip())
if 'Company_Name' in df_scores.columns:
    df_scores['Ticker'] = df_scores['Ticker'].fillna(df_scores['Company_Name'].astype(str).str.upper().str.strip())
if 'company_name' in df_edgar.columns:
    df_edgar['Ticker'] = df_edgar['Ticker'].fillna(df_edgar['company_name'].astype(str).str.upper().str.strip())

# Remove company name columns
if 'Company_Name' in df_scores.columns:
    df_scores = df_scores.drop(columns=['Company_Name'])
if 'company_name' in df_edgar.columns:
    df_edgar = df_edgar.drop(columns=['company_name'])

# merge DFs (left join)
print("Merging all dataframes...")
merged_step1 = pd.merge(df_fortune, df_scores, on='Ticker', how='left')
final_merged_df = pd.merge(merged_step1, df_edgar, on='Ticker', how='left')

# Revert the temporary "Company Name Tickers" back to blank
is_fallback = final_merged_df['Ticker'] == final_merged_df['COMPANY NAME'].astype(str).str.upper().str.strip()
final_merged_df.loc[is_fallback, 'Ticker'] = np.nan

# Convert any blank spaces to actual "NaN" (blanks) so Pandas can spot them
final_merged_df = final_merged_df.replace(r'^\s*$', np.nan, regex=True)

# Reorder columns
fortune_cols = [col for col in df_fortune.columns if col != 'Ticker']
pillars_cols = [col for col in df_scores.columns if col != 'Ticker']
edgar_cols   = [col for col in df_edgar.columns   if col != 'Ticker']

# Assemble the new column order
new_order = ['Ticker'] + fortune_cols + pillars_cols + edgar_cols

# Apply the new order to the dataframe
final_merged_df = final_merged_df[new_order]

# save the result to excel
output_filename = 'Merged_Data_Fortune500.xlsx'
print(f"Saving merged data to {output_filename}...")

# Save to .xlsx
final_merged_df.to_excel(output_filename, index=False)

print("Done! 🎉 Your file has exactly 500 rows and private companies are merged.")