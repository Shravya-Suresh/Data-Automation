"""Export module for CSV, Parquet, and Excel output.

Converts lists of :class:`PipelineRow` objects into flat tabular files
suitable for analysis in pandas, Excel, or any downstream tool.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Any

import pandas as pd

from edgar_tracker.models import PipelineRow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def rows_to_dataframe(rows: list[PipelineRow]) -> pd.DataFrame:
    """Convert a list of PipelineRow objects to a flat pandas DataFrame.

    Each row is flattened via :meth:`PipelineRow.to_flat_dict`, which merges
    the dynamic ``keyword_scores`` and ``ai_spend`` dicts into the top-level
    columns alongside the fixed filing metadata fields.

    Parameters
    ----------
    rows:
        Pipeline output rows.  May be empty, in which case an empty
        DataFrame with the correct columns is returned.

    Returns
    -------
    pd.DataFrame
        A DataFrame with one row per filing and all score columns expanded.
    """
    if not rows:
        # Return an empty DataFrame that still has the base column names so
        # downstream writers produce a file with headers.
        sample = PipelineRow()
        return pd.DataFrame(columns=list(sample.to_flat_dict().keys()))

    records: list[dict[str, Any]] = [row.to_flat_dict() for row in rows]
    return pd.DataFrame(records)


def generate_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Build a company-level summary pivot from a filing-level DataFrame.

    The summary shows, for every ``company_name``, the mean of each
    ``count_*`` column (i.e. keyword-group total counts).  Only columns
    whose name starts with ``count_`` and does *not* contain a section
    suffix (no second underscore after the group name) are included so that
    the pivot stays compact.

    Parameters
    ----------
    df:
        A DataFrame produced by :func:`rows_to_dataframe`.

    Returns
    -------
    pd.DataFrame
        Pivot table indexed by ``company_name`` with one column per keyword
        group showing the mean mention count across that company's filings.
    """
    if df.empty or "company_name" not in df.columns:
        return pd.DataFrame()

    # Identify top-level group count columns (e.g. count_ai, count_digital)
    # but exclude per-section or per-keyword columns.
    count_cols: list[str] = [
        c for c in df.columns
        if c.startswith("count_")
        and c not in ("count_", )
    ]

    if not count_cols:
        return pd.DataFrame()

    # Coerce to numeric so the pivot works even if data came from CSV reload.
    numeric_df = df[["company_name"] + count_cols].copy()
    for col in count_cols:
        numeric_df[col] = pd.to_numeric(numeric_df[col], errors="coerce")

    summary: pd.DataFrame = (
        numeric_df
        .groupby("company_name", sort=True)
        .mean(numeric_only=True)
    )

    return summary


# ---------------------------------------------------------------------------
# Boolean formatting helper
# ---------------------------------------------------------------------------


def _booleans_to_yes_no(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy where boolean columns are replaced with 'Yes'/'No' strings."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == "bool" or set(out[col].dropna().unique()).issubset({True, False}):
            out[col] = out[col].map({True: "Yes", False: "No", None: ""})
    return out


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------


def export_csv(rows: list[PipelineRow], output_path: str | Path) -> Path:
    """Export pipeline results to a CSV file.

    Parameters
    ----------
    rows:
        Pipeline output rows.  An empty list writes headers only.
    output_path:
        Destination file path (will be created / overwritten).

    Returns
    -------
    Path
        The resolved path to the written file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df: pd.DataFrame = rows_to_dataframe(rows)
    df.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
        lineterminator="\n",
    )

    return output_path.resolve()


# ---------------------------------------------------------------------------
# Parquet export
# ---------------------------------------------------------------------------


def export_parquet(rows: list[PipelineRow], output_path: str | Path) -> Path:
    """Export pipeline results to a Parquet file.

    Uses the ``pyarrow`` engine for broad compatibility and efficient
    columnar storage.

    Parameters
    ----------
    rows:
        Pipeline output rows.  An empty list writes a schema-only file.
    output_path:
        Destination file path (will be created / overwritten).

    Returns
    -------
    Path
        The resolved path to the written file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df: pd.DataFrame = rows_to_dataframe(rows)
    df.to_parquet(output_path, engine="pyarrow", index=False)

    return output_path.resolve()


# ---------------------------------------------------------------------------
# Excel export
# ---------------------------------------------------------------------------


def _auto_size_columns(ws: Any) -> None:
    """Set each column width to fit the longest cell value (capped at 60).

    Parameters
    ----------
    ws:
        An openpyxl ``Worksheet`` object.
    """
    for column_cells in ws.columns:
        max_length: int = 0
        column_letter: str = column_cells[0].column_letter
        for cell in column_cells:
            try:
                cell_len = len(str(cell.value)) if cell.value is not None else 0
                if cell_len > max_length:
                    max_length = cell_len
            except (TypeError, AttributeError):
                pass
        # Add a small padding and cap at 60 characters.
        adjusted_width: float = min(max_length + 3, 60)
        ws.column_dimensions[column_letter].width = adjusted_width


def _freeze_top_row(ws: Any) -> None:
    """Freeze the first row so headers remain visible when scrolling.

    Parameters
    ----------
    ws:
        An openpyxl ``Worksheet`` object.
    """
    ws.freeze_panes = "A2"


def export_excel(rows: list[PipelineRow], output_path: str | Path) -> Path:
    """Export pipeline results to an Excel workbook with formatted sheets.

    The workbook contains three sheets:

    1. **Filing Scores** -- every column from the flat pipeline output, with
       boolean columns rendered as "Yes"/"No".
    2. **Summary by Company** -- a pivot table with companies as rows and
       keyword-group mean counts as columns.
    3. **Metadata** -- run-level information (date, company count, filing
       count, etc.).

    All sheets have auto-sized column widths and frozen top rows.

    Parameters
    ----------
    rows:
        Pipeline output rows.  An empty list produces a workbook with
        headers only (sheets 1 & 2) and zeroed metadata.
    output_path:
        Destination ``.xlsx`` file path (will be created / overwritten).

    Returns
    -------
    Path
        The resolved path to the written file.
    """
    from openpyxl.utils.dataframe import dataframe_to_rows as _df_to_rows

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df: pd.DataFrame = rows_to_dataframe(rows)
    summary_df: pd.DataFrame = generate_summary(df)

    # -- Prepare display versions with Yes/No booleans ----------------------
    display_df: pd.DataFrame = _booleans_to_yes_no(df)

    # -- Gather metadata ----------------------------------------------------
    unique_companies: int = df["company_name"].nunique() if not df.empty else 0
    filing_count: int = len(df)
    taxonomy_groups: list[str] = sorted(
        {
            col.replace("contains_", "")
            for col in df.columns
            if col.startswith("contains_")
            and "_item" not in col  # exclude section-level flags
        }
    )

    meta_records: list[dict[str, str]] = [
        {"Field": "Export Date (UTC)", "Value": _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")},
        {"Field": "Company Count", "Value": str(unique_companies)},
        {"Field": "Filing Count", "Value": str(filing_count)},
        {"Field": "Taxonomy Groups", "Value": ", ".join(taxonomy_groups) if taxonomy_groups else "(none)"},
    ]
    meta_df: pd.DataFrame = pd.DataFrame(meta_records)

    # -- Write workbook with openpyxl via pandas ExcelWriter ----------------
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Sheet 1: Filing Scores
        display_df.to_excel(writer, sheet_name="Filing Scores", index=False)

        # Sheet 2: Summary by Company
        if not summary_df.empty:
            summary_df.to_excel(writer, sheet_name="Summary by Company")
        else:
            pd.DataFrame({"(no data)": []}).to_excel(
                writer, sheet_name="Summary by Company", index=False,
            )

        # Sheet 3: Metadata
        meta_df.to_excel(writer, sheet_name="Metadata", index=False)

        # -- Post-write formatting ------------------------------------------
        workbook = writer.book
        for ws in workbook.worksheets:
            _auto_size_columns(ws)
            _freeze_top_row(ws)

    return output_path.resolve()
