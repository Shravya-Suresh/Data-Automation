# Fortune 500 Merge Pipeline

Combines three data sources into the final `Fortune500_Complete.xlsx` deliverable:
the company information / financials (from the professor), the Digital Maturity
pillar scores (from the 10-K text-analysis pipeline), and the AI / EDGAR analysis
(from a colleague). The output has two sheets — a one-row-per-company averaged
master and a one-row-per-company-year breakdown.

The pillar scores now include the seventh pillar, **Governance**, and use
**Personalization** as the sixth pillar (the former `People` column).

## Repository structure

```
merge_pipeline/
├── scripts/
│   ├── 01_average_pillars.py     # average pillar scores per company
│   ├── 02_build_master.py        # company info + averages + AI intensity
│   └── 03_assemble_complete.py   # yearly breakdown + final 2-sheet workbook
├── inputs/                       # given source files (see below)
│   ├── seven_pillars_data.xlsx
│   ├── 2025_Fortune_500_financials.xlsx
│   ├── edgar_results_2026-02-23.xlsx
│   ├── average_pillars_scores_and_ai_intensity.xlsx
│   └── company_info.xlsx
├── results/                      # generated output (created by the scripts)
│   ├── Fortune500_Complete.xlsx  # <- the deliverable
│   └── intermediate/             # intermediate step outputs
│       ├── average_pillars_scores.xlsx
│       └── master_averaged.xlsx
├── requirements.txt
└── README.md
```

Scripts resolve their paths from the project root, so they run from any working
directory.

## Inputs (provided, not generated here)

These come from outside this folder and are treated as given:

- **`seven_pillars_data.xlsx`** — per-filing Digital Maturity pillar scores from
  the 10-K text-analysis pipeline (one row per company-filing, seven pillars).
- **`2025_Fortune_500_financials.xlsx`** — Fortune 500 financials (professor).
- **`edgar_results_2026-02-23.xlsx`** — AI / keyword analysis per filing
  (colleague); the `Filing Scores` sheet is used.
- **`average_pillars_scores_and_ai_intensity.xlsx`** — source of the
  `Avg_AI_Intensity` column (colleague); used as-is, not recomputed.
- **`company_info.xlsx`** — static company information including contact fields
  (Address, Telephone, Corporate Website). These fields exist only in the
  approved master, so this file was extracted from it once and is treated as a
  fixed company-info input.

## The pipeline

Run in order from the project root:

```bash
pip install -r requirements.txt
python scripts/01_average_pillars.py
python scripts/02_build_master.py
python scripts/03_assemble_complete.py
```

**Phase 1 — Average pillars.** Reads `seven_pillars_data.xlsx`, averages each
pillar's 0–100 score across a company's filing years, and renames the columns to
`Avg_<PILLAR>`. Pillars are detected dynamically, so Governance and the
Personalization rename require no code changes.

**Phase 2 — Build master.** Joins `company_info.xlsx` with the pillar averages
and the `Avg_AI_Intensity` column on `Ticker` (left join, keeping all 500
companies) and orders the columns: company info → pillar averages → AI intensity.

**Phase 3 — Assemble.** Reduces the pillar scores and EDGAR 10-K rows to one row
per `Ticker`/`Year`, merges them with the Fortune 500 company info to form the
yearly breakdown, and writes the final workbook with two sheets:

- **`Master_Averaged`** — 500 rows, one per company: company info + the seven
  `Avg_` pillar scores + `Avg_AI_Intensity` (39 columns).
- **`Yearly_Breakdown`** — one row per company-year: company info + per-year
  pillar scores (seven pillars, score + raw) + the colleague's EDGAR/AI columns,
  joined on `Ticker` + `Year`.

## Notes

- **AI intensity is carried through unchanged** — it is independent of the
  pillars, so adding Governance does not affect it.
- **The financials file has 502 rows**, two of which are footnote rows with no
  company name; they drop out on save, leaving 1,878 yearly rows (matching the
  previously approved file).
- **Documentation sheets** are added to the deliverable separately and are not
  produced by this pipeline.
- `Merged_Data_Fortune500.xlsx` from the original folder is unused by any step
  and was intentionally left out.
