"""
Merge Phase 2: Build the per-company master table

Joins the static company information (Fortune financials + contact details) with
the averaged pillar scores and the AI-intensity score (one row per company). This
becomes the "Master_Averaged" sheet of the final workbook.

Inputs : inputs/company_info.xlsx                          (company info + contact)
         results/intermediate/average_pillars_scores.xlsx (Avg_* pillar scores)
         inputs/average_pillars_scores_and_ai_intensity.xlsx  (Avg_AI_Intensity)
Output : results/intermediate/master_averaged.xlsx
"""

import os
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
INPUTS = os.path.join(PROJECT_ROOT, 'inputs')
INTERMEDIATE = os.path.join(PROJECT_ROOT, 'results', 'intermediate')

COMPANY_FILE = os.path.join(INPUTS, 'company_info.xlsx')
SCORES_FILE = os.path.join(INTERMEDIATE, 'average_pillars_scores.xlsx')
AI_FILE = os.path.join(INPUTS, 'average_pillars_scores_and_ai_intensity.xlsx')
OUTPUT_FILE = os.path.join(INTERMEDIATE, 'master_averaged.xlsx')


def norm_ticker(s):
    return s.astype(str).str.upper().str.strip()


def main():
    os.makedirs(INTERMEDIATE, exist_ok=True)
    company = pd.read_excel(COMPANY_FILE)
    scores = pd.read_excel(SCORES_FILE)
    ai = pd.read_excel(AI_FILE, sheet_name='Fortune500')[['Ticker', 'Avg_AI_Intensity']]

    # Standardize the join key
    for df in (company, scores, ai):
        df['Ticker'] = norm_ticker(df['Ticker'])

    # Pillar averages only (drop the duplicate company-name column from the scores file)
    avg_cols = [c for c in scores.columns if c.startswith('Avg_')]
    scores = scores[['Ticker'] + avg_cols]

    # Left-join onto the full company list so all companies are kept
    master = company.merge(scores, on='Ticker', how='left').merge(ai, on='Ticker', how='left')

    # Column order: company info -> pillar averages -> AI intensity
    company_cols = [c for c in company.columns]
    master = master[company_cols + avg_cols + ['Avg_AI_Intensity']]

    master.to_excel(OUTPUT_FILE, index=False)
    print(f"Master built: {master.shape[0]} companies, {master.shape[1]} columns.")
    print("Score columns:", avg_cols + ['Avg_AI_Intensity'])
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
