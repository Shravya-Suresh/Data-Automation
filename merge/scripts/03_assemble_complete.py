"""
Merge Phase 3: Assemble the final deliverable

Builds the per-filing-year breakdown (company info + pillar scores + the
colleague's EDGAR/AI columns, joined on Ticker + Year) and writes the final
workbook with two sheets:
  - Master_Averaged : one row per company (from Phase 2)
  - Yearly_Breakdown: one row per company-year

Inputs : results/intermediate/master_averaged.xlsx  (Phase 2 output)
         inputs/2025_Fortune_500_financials.xlsx
         inputs/seven_pillars_data.xlsx
         inputs/edgar_results_2026-02-23.xlsx   ('Filing Scores' sheet)
Output : results/Fortune500_Complete.xlsx
"""

import os
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
INPUTS = os.path.join(PROJECT_ROOT, 'inputs')
RESULTS = os.path.join(PROJECT_ROOT, 'results')

MASTER_FILE = os.path.join(RESULTS, 'intermediate', 'master_averaged.xlsx')
F500_FILE = os.path.join(INPUTS, '2025_Fortune_500_financials.xlsx')
PILLARS_FILE = os.path.join(INPUTS, 'seven_pillars_data.xlsx')
EDGAR_FILE = os.path.join(INPUTS, 'edgar_results_2026-02-23.xlsx')
OUTPUT_FILE = os.path.join(RESULTS, 'Fortune500_Complete.xlsx')


def norm_ticker(s):
    return s.astype(str).str.upper().str.strip()


def main():
    print("Loading datasets...")
    df_master = pd.read_excel(MASTER_FILE)
    df_f500 = pd.read_excel(F500_FILE).rename(columns={'TICKER': 'Ticker'})
    df_pillars = pd.read_excel(PILLARS_FILE)
    df_edgar = pd.read_excel(EDGAR_FILE, sheet_name='Filing Scores')

    # Fortune 500 company info kept on the yearly sheet (+ rank for ordering)
    f500_cols = ['COMPANY NAME', 'CITY', 'STATE', 'Industry', 'Ticker', 'Revenue Rank 2024']
    f500_cols = [c for c in f500_cols if c in df_f500.columns]
    df_f500 = df_f500[f500_cols]
    df_f500['Ticker'] = norm_ticker(df_f500['Ticker'])

    # Pillars -> one row per Ticker/Year (keep the latest filing in a year)
    df_pillars['Ticker'] = norm_ticker(df_pillars['Ticker'])
    df_pillars['Date_Filed'] = pd.to_datetime(df_pillars['Date_Filed'], errors='coerce')
    df_pillars['Year'] = df_pillars['Date_Filed'].dt.year
    df_pillars = (df_pillars.sort_values('Date_Filed')
                  .drop_duplicates(subset=['Ticker', 'Year'], keep='last'))

    # EDGAR -> 10-K filings only, one row per Ticker/Year
    df_edgar = df_edgar[df_edgar['form'] == '10-K'].copy()
    df_edgar = df_edgar.rename(columns={'ticker': 'Ticker'})
    df_edgar['Ticker'] = norm_ticker(df_edgar['Ticker'])
    df_edgar['filing_date'] = pd.to_datetime(df_edgar['filing_date'], errors='coerce')
    df_edgar['Year'] = df_edgar['filing_date'].dt.year
    df_edgar = (df_edgar.sort_values('filing_date')
                .drop_duplicates(subset=['Ticker', 'Year'], keep='last'))

    print("Merging yearly data...")
    yearly = pd.merge(df_pillars, df_edgar, on=['Ticker', 'Year'], how='outer')
    sheet2 = pd.merge(df_f500, yearly, on='Ticker', how='left')

    # Fortune 500 order, then chronological; drop the helper rank column
    if 'Revenue Rank 2024' in sheet2.columns:
        sheet2 = sheet2.sort_values(by=['Revenue Rank 2024', 'Ticker', 'Year'])
        sheet2 = sheet2.drop(columns=['Revenue Rank 2024'])
        f500_cols.remove('Revenue Rank 2024')

    # Column order: F500 info -> pillar columns -> EDGAR columns
    pillars_cols = [c for c in df_pillars.columns if c not in f500_cols]
    edgar_cols = [c for c in df_edgar.columns if c not in f500_cols and c not in pillars_cols]
    order = [c for c in (f500_cols + pillars_cols + edgar_cols) if c in sheet2.columns]
    sheet2 = sheet2[order]

    print("Writing workbook...")
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        df_master.to_excel(writer, sheet_name='Master_Averaged', index=False)
        sheet2.to_excel(writer, sheet_name='Yearly_Breakdown', index=False)

    print(f"Done. Master_Averaged: {df_master.shape}, Yearly_Breakdown: {sheet2.shape}")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
