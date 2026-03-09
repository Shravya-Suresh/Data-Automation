import pandas as pd

def create_final_deliverable():
    print("Loading datasets...")
    # 1. Corrected filename to ensure Sheet 1 is an exact copy
    df_master = pd.read_excel('Merged_Fortune_500_scores.xlsx') 
    
    df_f500 = pd.read_excel('2025.Fortune.500.financials.xlsx')
    df_pillars = pd.read_excel('six_pillars_data.xlsx')
    df_edgar = pd.read_excel('edgar_results_2026-02-23.xlsx') 

    print("Filtering Fortune 500 Financials...")
    df_f500 = df_f500.rename(columns={'TICKER': 'Ticker'})
    
    # Keep only the requested columns, plus the Rank temporarily for sorting
    f500_cols_to_keep = ['COMPANY NAME', 'CITY', 'STATE', 'Industry', 'Ticker', 'Revenue Rank 2024']
    f500_cols_to_keep = [col for col in f500_cols_to_keep if col in df_f500.columns]
    df_f500 = df_f500[f500_cols_to_keep]

    print("Processing Dates into Years...")
    df_pillars['Date_Filed'] = pd.to_datetime(df_pillars['Date_Filed'], errors='coerce')
    df_pillars['Year'] = df_pillars['Date_Filed'].dt.year

    df_edgar['filing_date'] = pd.to_datetime(df_edgar['filing_date'], errors='coerce')
    df_edgar['Year'] = df_edgar['filing_date'].dt.year
    df_edgar = df_edgar.rename(columns={'ticker': 'Ticker'})
    
    # FIX FOR THE 16 ROWS ISSUE: 
    # Sort by date and drop duplicates so we only keep exactly one filing per year per Ticker.
    df_pillars = df_pillars.sort_values('Date_Filed').drop_duplicates(subset=['Ticker', 'Year'], keep='last')
    df_edgar = df_edgar.sort_values('filing_date').drop_duplicates(subset=['Ticker', 'Year'], keep='last')

    print("Merging Yearly Data...")
    yearly_combined = pd.merge(df_pillars, df_edgar, on=['Ticker', 'Year'], how='outer')

    print("Integrating with Fortune 500 Base...")
    sheet2_df = pd.merge(df_f500, yearly_combined, on='Ticker', how='left')

    # Sort to keep the Fortune 500 order and chronological years, then drop the rank column
    if 'Revenue Rank 2024' in sheet2_df.columns:
        sheet2_df = sheet2_df.sort_values(by=['Revenue Rank 2024', 'Ticker', 'Year'])
        sheet2_df = sheet2_df.drop(columns=['Revenue Rank 2024'])
        f500_cols_to_keep.remove('Revenue Rank 2024')

    print("Reordering Columns...")
    # Get Pillars columns (excluding ones already captured)
    pillars_cols = [col for col in df_pillars.columns if col not in f500_cols_to_keep]
    
    # Get Edgar columns (excluding ones already captured)
    edgar_cols = [col for col in df_edgar.columns if col not in f500_cols_to_keep and col not in pillars_cols]

    final_col_order = f500_cols_to_keep + pillars_cols + edgar_cols
    # Safety check to ensure all columns exist before reordering
    final_col_order = [col for col in final_col_order if col in sheet2_df.columns]
    
    sheet2_df = sheet2_df[final_col_order]

    print("Exporting to Excel...")
    output_filename = 'Fortune500_Complete.xlsx'
    
    with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
        # Sheet 1: Exact copy of Merged_Fortune_500_scores
        df_master.to_excel(writer, sheet_name='Master_Averaged', index=False)
        # Sheet 2: The cleaned Yearly breakdown (4 rows per company)
        sheet2_df.to_excel(writer, sheet_name='Yearly_Breakdown', index=False)
        
    print(f"Success! File saved as: {output_filename}")

if __name__ == '__main__':
    create_final_deliverable()