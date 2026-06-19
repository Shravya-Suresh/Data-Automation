"""
Merge Phase 1: Average the pillar scores per company

Reads the per-filing pillar scores and collapses each company's multiple years
into a single average per pillar. Pillar columns are detected dynamically, so
the seventh pillar (Governance) and the Personalization rename flow through
automatically.

Input : inputs/seven_pillars_data.xlsx                    (one row per filing)
Output: results/intermediate/average_pillars_scores.xlsx  (one row per company)
"""

import os
import re
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
INTERMEDIATE_DIR = os.path.join(PROJECT_ROOT, 'results', 'intermediate')
INPUT_FILE = os.path.join(PROJECT_ROOT, 'inputs', 'seven_pillars_data.xlsx')
OUTPUT_FILE = os.path.join(INTERMEDIATE_DIR, 'average_pillars_scores.xlsx')


def main():
    os.makedirs(INTERMEDIATE_DIR, exist_ok=True)
    df = pd.read_excel(INPUT_FILE)

    # Every 0-100 pillar score column (PURPOSE_Score_0_to_100, ... GOVERNANCE_Score_0_to_100)
    score_cols = [c for c in df.columns if 'Score_0_to_100' in c]

    # Average each pillar across a company's filing years
    avg_df = df.groupby(['Company_Name', 'Ticker'])[score_cols].mean().reset_index()

    # Rename PURPOSE_Score_0_to_100 -> Avg_PURPOSE (strip any legacy DIMENSION_n_ prefix too)
    renames = {}
    for col in score_cols:
        clean = re.sub(r'DIMENSION_\d+_', '', col).replace('_Score_0_to_100', '')
        renames[col] = f'Avg_{clean}'
    avg_df = avg_df.rename(columns=renames)

    avg_df.to_excel(OUTPUT_FILE, index=False)
    print(f"Averaged {len(avg_df)} companies across {len(score_cols)} pillars.")
    print("Pillar columns:", [renames[c] for c in score_cols])
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
