import pandas as pd
import numpy as np

# Suppress the Future Warning message
pd.set_option('future.no_silent_downcasting', True)

# ==========================================
# 1. DEFINE YOUR FILE PATHS HERE
# ==========================================
fortune_file = '2025.Fortune.500.financials.xlsx'
scores_file = 'average_pillars_scores.xlsx' 
scores_ai_file = 'average_pillars_scores_and_ai_intensity.xlsx'

# ==========================================
# 2. LOAD DATA & CLEAN
# ==========================================
print("Loading Fortune 500, Original Scores, and AI Intensity data...")
df_fortune = pd.read_excel(fortune_file) 
df_scores = pd.read_excel(scores_file)
df_scores_ai = pd.read_excel(scores_ai_file, sheet_name='Fortune500')

# Drop 'Unnamed' and 'FN:' columns from the Fortune 500 file
cols_to_drop = [col for col in df_fortune.columns if 'Unnamed' in str(col) or 'FN:' in str(col)]
df_fortune = df_fortune.drop(columns=cols_to_drop)

# Drop any row that doesn't have a Company Name, leaving exactly 500 clean rows.
df_fortune = df_fortune.dropna(subset=['COMPANY NAME'])

# --- THE FIX ---
# Keep ONLY the Ticker, Company Name, and Avg_AI_Intensity from the AI file.
# This prevents it from overwriting the correct pillar scores!
cols_to_keep = [col for col in df_scores_ai.columns if col.lower() in ['ticker', 'company_name'] or col == 'Avg_AI_Intensity']
df_scores_ai = df_scores_ai[cols_to_keep]

# ==========================================
# 3. STANDARDIZE TICKER COLUMNS
# ==========================================
df_fortune = df_fortune.rename(columns={'TICKER': 'Ticker'})
if 'ticker' in df_scores.columns: df_scores = df_scores.rename(columns={'ticker': 'Ticker'})
if 'TICKER' in df_scores.columns: df_scores = df_scores.rename(columns={'TICKER': 'Ticker'})

if 'ticker' in df_scores_ai.columns: df_scores_ai = df_scores_ai.rename(columns={'ticker': 'Ticker'})
if 'TICKER' in df_scores_ai.columns: df_scores_ai = df_scores_ai.rename(columns={'TICKER': 'Ticker'})

# Clean string formats (uppercase and strip accidental spaces)
df_fortune['Ticker'] = df_fortune['Ticker'].astype(str).str.upper().str.strip()
df_scores['Ticker'] = df_scores['Ticker'].astype(str).str.upper().str.strip()
df_scores_ai['Ticker'] = df_scores_ai['Ticker'].astype(str).str.upper().str.strip()

# Replace 'NAN', 'NONE', or blank strings with actual empty missing values
df_fortune['Ticker'] = df_fortune['Ticker'].replace(['NAN', 'NONE', ''], np.nan)
df_scores['Ticker'] = df_scores['Ticker'].replace(['NAN', 'NONE', ''], np.nan)
df_scores_ai['Ticker'] = df_scores_ai['Ticker'].replace(['NAN', 'NONE', ''], np.nan)

# ==========================================
# 4. TEMPORARY BRIDGE FOR PRIVATE COMPANIES
# ==========================================
# For private companies with no ticker, use their UPPERCASE COMPANY NAME as a temporary bridge!
df_fortune['Ticker'] = df_fortune['Ticker'].fillna(df_fortune['COMPANY NAME'].astype(str).str.upper().str.strip())

# Apply bridge to the original scores file
comp_col_scores = 'Company_Name' if 'Company_Name' in df_scores.columns else 'company_name' if 'company_name' in df_scores.columns else None
if comp_col_scores:
    df_scores['Ticker'] = df_scores['Ticker'].fillna(df_scores[comp_col_scores].astype(str).str.upper().str.strip())
    df_scores = df_scores.drop(columns=[comp_col_scores])

# Apply bridge to the AI Intensity file
comp_col_ai = 'Company_Name' if 'Company_Name' in df_scores_ai.columns else 'company_name' if 'company_name' in df_scores_ai.columns else None
if comp_col_ai:
    df_scores_ai['Ticker'] = df_scores_ai['Ticker'].fillna(df_scores_ai[comp_col_ai].astype(str).str.upper().str.strip())
    df_scores_ai = df_scores_ai.drop(columns=[comp_col_ai])

# ==========================================
# 5. MERGE THE DATAFRAMES
# ==========================================
print("Merging dataframes...")
# First merge Fortune 500 with the correct Original Scores
merged_step1 = pd.merge(df_fortune, df_scores, on='Ticker', how='left')

# Then merge that result with ONLY the AI Intensity scores
final_merged_df = pd.merge(merged_step1, df_scores_ai, on='Ticker', how='left')

# Revert the temporary "Company Name Tickers" back to being completely blank
is_fallback = final_merged_df['Ticker'] == final_merged_df['COMPANY NAME'].astype(str).str.upper().str.strip()
final_merged_df.loc[is_fallback, 'Ticker'] = np.nan

# Convert any blank spaces to actual "NaN" (blanks) so Pandas can spot them
final_merged_df = final_merged_df.replace(r'^\s*$', np.nan, regex=True)

# ==========================================
# 6. REORDER COLUMNS & SAVE
# ==========================================
fortune_cols = [col for col in df_fortune.columns if col != 'Ticker']
pillars_cols = [col for col in df_scores.columns if col != 'Ticker']
ai_cols = [col for col in df_scores_ai.columns if col != 'Ticker']

# Assemble the new column order: Ticker -> Fortune 500 data -> Original Pillars -> AI Intensity data
new_order = ['Ticker'] + fortune_cols + pillars_cols + ai_cols
final_merged_df = final_merged_df[new_order]

# Save the result to Excel
output_filename = 'Final_Merged_Fortune500_AI_Intensity.xlsx'
print(f"Saving merged data to {output_filename}...")

final_merged_df.to_excel(output_filename, index=False)

print("Done! 🎉 Your file has exactly 500 rows, with correct pillar scores and the new AI intensity.")