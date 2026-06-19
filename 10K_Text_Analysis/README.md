# Digital Maturity Pipeline — SEC EDGAR 10-K Text Analysis

An end-to-end Python pipeline that downloads, cleans, and scores Fortune 500
SEC 10-K filings to produce a multi-year **Digital Maturity** index. It bridges
stock ticker symbols and SEC Central Index Keys (CIKs) to fetch the right
filings, cleans the narrative text using the Loughran & McDonald methodology,
and scores each filing across seven themes — six digital-transformation
"P's" plus a Governance pillar.

The textual cleaning follows the Internet Appendix of *"When is a Liability not
a Liability? Textual Analysis, Dictionaries, and 10-Ks"* by Tim Loughran and
Bill McDonald. The scoring dictionary is a custom six-dimension keyword set
built for this project (it is **not** the LM sentiment dictionary).

## Repository structure

```
digital-maturity-pipeline/
├── scripts/
│   ├── 01_mapping.py      # Phase 1: ticker -> CIK -> 10-K download URLs
│   ├── 02_fetching.py     # Phase 2: download raw 10-K filings
│   ├── 03_cleaning.py     # Phase 3: clean filings into narrative text
│   └── 04_scoring.py      # Phase 4: score filings on Digital Maturity
├── results/
│   ├── company_tickers_and_names.csv   # input: target company list
│   ├── Target_List_MultiYear.csv       # Phase 1 output: mapped filings + URLs
│   └── seven_pillars_data.xlsx         # Phase 4 output (generated)
├── 10k_filings/        # Phase 2 output: raw .txt filings (generated)
├── cleaned_filings/    # Phase 3 output: cleaned .txt filings (generated)
├── requirements.txt
├── .env                # not committed — holds SEC_EMAIL (see Setup)
└── README.md
```

Scripts resolve their input/output paths relative to the project root, so they
can be run from any working directory. The two large data folders
(`10k_filings/` and `cleaned_filings/`) are created at the project root during
the run.

## The pipeline

The four phases run in order, each consuming the previous phase's output:

| Phase | Script        | Input                              | Output                          |
|-------|---------------|------------------------------------|---------------------------------|
| 1     | `01_mapping.py`  | `results/company_tickers_and_names.csv` | `results/Target_List_MultiYear.csv` |
| 2     | `02_fetching.py` | `results/Target_List_MultiYear.csv`     | `10k_filings/`                  |
| 3     | `03_cleaning.py` | `10k_filings/`                     | `cleaned_filings/`              |
| 4     | `04_scoring.py`  | `cleaned_filings/` + mapping file  | `results/seven_pillars_data.xlsx` |

**Phase 1 — Mapping.** Loads the target company list, fills in placeholder
tickers for entities without one, and maps each ticker to its 10-digit SEC CIK
using the official SEC `company_tickers.json`, patched with hardcoded overrides
for recent mergers and private/mutual companies. It then crawls the SEC
quarterly master index for every quarter of the target years, keeps the rows
whose form type is exactly `10-K` and whose CIK is in the target set, and writes
the exact download URL for each filing.

**Phase 2 — Fetching.** Downloads each raw `.txt` filing from SEC EDGAR into
`10k_filings/`, named `TICKER_YYYY-MM-DD.txt`. It throttles requests to respect
SEC rate limits, retries once on a 403/429, and skips files already on disk, so
an interrupted run can be resumed.

**Phase 3 — Cleaning.** For each raw filing, removes SEC/IMS headers and Inline
XBRL metadata, drops exhibit/XML/graphic/zip/excel/json sub-documents, deletes
financial tables (those that are more than 25% digits) while keeping text
tables, strips encoded image blobs, then uppercases the text and reduces it to
alphabetic-only words. Runs in parallel across CPU cores and is resumable.

**Phase 4 — Scoring.** Pre-cleans the keyword dictionary the same way the text
was cleaned, compiles word-boundary regexes per dimension, and counts matches in
each cleaned filing. Counts are converted to term frequency (matches / total
words), then **min-max normalized across the whole dataset** to a 0–100 score
per dimension. Each dimension is normalized independently, so adding or
removing a pillar does not change the other pillars' scores. Company names are joined back from the mapping file, and results
are written to `results/seven_pillars_data.xlsx`.

### The seven dimensions (six "P's" plus Governance)

1. **Purpose** — mission, sustainability/ESG, social responsibility, trust
2. **Phygital** — omnichannel, click-and-collect, IoT, connected stores
3. **Platforms** — platform/marketplace/ecosystem, API, network effects
4. **Participation** — co-creation, crowdsourcing, user-generated content
5. **Partnerships** — alliances, joint ventures, innovation ecosystems
6. **Personalization** — digital skills, upskilling, agile, digital culture
7. **Governance** — oversight, risk mitigation, board committee, governance
   architecture, safeguards, compliance

## Setup

A virtual environment is recommended.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

The SEC requires a descriptive `User-Agent` containing a contact email. Create a
`.env` file in the project root:

```
SEC_EMAIL=your_name@your_domain.com
```

If `.env` is missing, the scripts fall back to a generic placeholder email,
which SEC may rate-limit or block.

## Usage

Run the phases in order from the project root:

```bash
python scripts/01_mapping.py     # Phase 1  (network: SEC)
python scripts/02_fetching.py    # Phase 2  (network: SEC; slow — many filings)
python scripts/03_cleaning.py    # Phase 3  (CPU-bound, parallel)
python scripts/04_scoring.py     # Phase 4  -> results/seven_pillars_data.xlsx
```

Phases 1 and 2 require internet access to SEC EDGAR. Phases 3 and 4 run entirely
on local files. Phases 2 and 3 are resumable.

## Dataset summary

The input company list contains ~502 companies. After mapping, **468 companies**
are matched to a CIK, yielding **1,848 10-K filings** across four filing years:

| Filing year | Filings |
|-------------|---------|
| 2022        | 456     |
| 2023        | 461     |
| 2024        | 463     |
| 2025        | 468     |

Note: these are **filing** years, not fiscal years. A 10-K filed in early 2022
generally covers fiscal year 2021.

## Notes and caveats

- **Min-max scoring is relative to the sample.** Each 0–100 score depends on the
  minimum and maximum term frequency observed across all filings in the dataset,
  so scores shift if companies are added or removed. The score is a within-sample
  ranking, not an absolute measure.
- **Form filter is exact.** Only `Form Type == '10-K'` is kept; amendments
  (`10-K/A`) and variants are excluded by design.
- **Overrides are manual.** Phase 1 includes a hardcoded CIK override table for
  companies the automatic mapping misses; it may need updating over time.