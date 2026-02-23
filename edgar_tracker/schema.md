# Output Dataset Schema

## Overview

The EDGAR Tracker pipeline processes SEC 10-K and 10-Q filings for a
configurable universe of public companies. For each filing it:

1. **Retrieves** the filing HTML from EDGAR (via the SEC EDGAR full-text
   submission API).
2. **Parses** the HTML into clean plain text and attempts to extract
   standard sections (Item 1, Item 1A, Item 7, Item 7A, Item 1C).
3. **Scores** the text against a YAML-defined keyword taxonomy, producing
   binary presence flags and mention counts at both the full-document and
   per-section level.
4. **Analyzes AI spend** by combining XBRL financial facts with
   text-derived evidence of AI investment activity.
5. **Exports** the results as a flat dataset (CSV, Parquet, or Excel).

Each row in the output represents a single filing. The columns are divided
into the categories described below.

---

## Column Reference

### Filing Metadata

| Column | Type | Description | Example |
|---|---|---|---|
| `company_name` | string | Full legal company name as reported to the SEC. | `APPLE INC` |
| `ticker` | string | Stock ticker symbol. | `AAPL` |
| `cik` | string | SEC Central Index Key, zero-padded to 10 digits. | `0000320193` |
| `form` | string | SEC form type. | `10-K` |
| `filing_date` | string (ISO date) | Date the filing was submitted to EDGAR. | `2024-11-01` |
| `report_date` | string (ISO date) | Period-of-report end date. | `2024-09-28` |
| `accession_number` | string | Unique EDGAR accession number (with dashes). | `0000320193-24-000123` |
| `filing_url` | string | Full URL to the primary filing document on EDGAR. | `https://www.sec.gov/Archives/edgar/data/...` |

### Parse Quality

| Column | Type | Description | Example |
|---|---|---|---|
| `section_parse_ok` | bool | Whether the section-level parser succeeded in extracting at least one standard section. Exported as `True`/`False` (CSV/Parquet) or `Yes`/`No` (Excel). | `True` |
| `token_count` | int | Approximate token count of the full filing text (whitespace-split). | `48532` |

### Keyword Score Columns -- Full Text

For each keyword group defined in the taxonomy (e.g. `ai`, `purpose`,
`digital`), two full-text columns are produced:

| Column Pattern | Type | Description | Example |
|---|---|---|---|
| `contains_{group}` | bool | `True` if any keyword in the group matched anywhere in the full filing text. | `contains_ai` = `True` |
| `count_{group}` | int | Total number of non-overlapping matches across all keywords in the group, over the entire filing text. | `count_ai` = `47` |

Additionally, per-keyword detail columns are produced:

| Column Pattern | Type | Description | Example |
|---|---|---|---|
| `count_{group}_{keyword_label}` | int | Match count for a single keyword within a group. Spaces and hyphens in the keyword label are replaced with underscores. | `count_ai_artificial_intelligence` = `12` |

### Keyword Score Columns -- Section Level

For each section that was successfully parsed (`item1`, `item1a`, `item7`,
`item7a`, `item1c`), section-level aggregates are produced per group:

| Column Pattern | Type | Description | Example |
|---|---|---|---|
| `contains_{group}_{section}` | bool | `True` if any keyword in the group matched within the specified section. | `contains_ai_item7` = `True` |
| `count_{group}_{section}` | int | Total match count for the group within the specified section. | `count_ai_item1a` = `9` |

**Standard section keys:**

| Key | SEC Section |
|---|---|
| `item1` | Item 1 -- Business |
| `item1a` | Item 1A -- Risk Factors |
| `item7` | Item 7 -- MD&A (Management's Discussion and Analysis) |
| `item7a` | Item 7A -- Quantitative and Qualitative Disclosures About Market Risk |
| `item1c` | Item 1C -- Cybersecurity |

Section-level columns are only present when at least one filing in the
dataset had the section successfully parsed. If a section could not be
extracted for a specific filing, its section-level values will be `False`
(for `contains_*`) and `0` (for `count_*`).

### AI Spend Columns

These columns capture structured and text-derived signals about AI-related
expenditure found in the filing.

| Column | Type | Description | Example |
|---|---|---|---|
| `ai_investment_mentions` | int | Number of text passages mentioning AI investment, spending, or budgets. | `3` |
| `ai_infrastructure_mentions` | int | Number of text passages referencing AI infrastructure (GPU clusters, data centers, cloud compute for ML). | `1` |
| `ai_spend_disclosure` | bool | `True` if the filing contains an explicit quantified AI spend disclosure. | `False` |
| `ai_intensity_score` | float | Heuristic score from 0.0 to 1.0 summarizing the overall intensity of AI spend signals in the filing. Higher values indicate more and stronger signals. | `0.3200` |
| `capex_total` | float or null | Total capital expenditures reported via XBRL, in USD. `null` if not available. | `11500000000.0` |
| `rd_expense` | float or null | Research & development expense reported via XBRL, in USD. `null` if not available. | `2950000000.0` |
| `software_intangibles` | float or null | Capitalized software / intangible assets reported via XBRL, in USD. `null` if not available. | `450000000.0` |
| `xbrl_facts_count` | int | Number of individual XBRL financial facts extracted for this filing. | `5` |
| `text_evidence_count` | int | Number of text-evidence snippets identified for AI spend activity. | `2` |

### Processing Metadata

| Column | Type | Description | Example |
|---|---|---|---|
| `status` | string | Processing outcome for this filing. `ok` on success; `error` if retrieval or parsing failed. | `ok` |
| `error` | string | Error message if `status` is `error`; empty string otherwise. | `HTTP 404` |
| `run_utc` | string (ISO datetime) | UTC timestamp of the pipeline run that produced this row. | `2025-06-15T14:30:00Z` |

---

## Notes on Data Types and Conventions

1. **Boolean columns** are stored as native Python `bool` (`True`/`False`)
   in CSV and Parquet. In the Excel export they are converted to the
   strings `Yes` and `No` for readability.

2. **Null / missing values.** Financial columns sourced from XBRL
   (`capex_total`, `rd_expense`, `software_intangibles`) may be `null`
   (represented as `NaN` in pandas, empty cell in CSV, `None` in Parquet).
   All other columns are guaranteed non-null.

3. **Column ordering.** Columns appear in the following order:
   - Filing metadata (company_name through filing_url)
   - Parse quality (section_parse_ok, token_count)
   - Full-text keyword scores (contains_*, count_*)
   - Per-keyword detail counts (count_{group}_{keyword})
   - Section-level keyword scores (contains_*_{section}, count_*_{section})
   - AI spend columns
   - Processing metadata (status, error, run_utc)

4. **Dynamic columns.** The exact set of keyword score columns depends on
   the taxonomy YAML used for the run. If the taxonomy defines groups
   `ai`, `purpose`, and `digital`, the output will contain `contains_ai`,
   `count_ai`, `contains_purpose`, `count_purpose`, etc. Adding a group to
   the taxonomy automatically adds the corresponding columns on the next
   run.

5. **Date strings** follow ISO 8601 (`YYYY-MM-DD` for dates,
   `YYYY-MM-DDTHH:MM:SSZ` for timestamps).

6. **CIK values** are stored as zero-padded 10-character strings (not
   integers) to preserve leading zeros.

7. **Parquet engine.** The Parquet export uses the `pyarrow` engine. All
   columns retain their pandas dtypes (`bool`, `int64`, `float64`,
   `object` for strings).

8. **Excel workbook structure.** The `.xlsx` export contains three sheets:
   - **Filing Scores** -- the full flat dataset with Yes/No booleans.
   - **Summary by Company** -- a pivot table with mean keyword-group
     counts per company.
   - **Metadata** -- run-level information (export date, company count,
     filing count, taxonomy groups used).
