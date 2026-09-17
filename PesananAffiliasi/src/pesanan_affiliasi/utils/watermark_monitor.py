# src/pesanan_affiliasi/utils/watermark_monitor.py
"""Watermark drift monitor — pre-flight gate for the daily ETL.

Compares the live GSheet state against the stored MinIO watermark to decide
whether each sheet has new data worth processing.

Gate rule: every sheet must have at least 1 toko group where live tanggal >
watermark date. If any sheet has 0 toko groups behind (or an access error),
the ETL aborts.
"""
import os
from datetime import datetime

import gspread
import pandas as pd

from src.pesanan_affiliasi.utils.gsheet_client import get_gspread_client, with_retry_on_429
from src.pesanan_affiliasi.utils.minio_client import get_minio_client, get_sheet_watermarks
from src.pesanan_affiliasi.utils.transform_utils import parse_mixed_dates, to_snake_case


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def col_index_to_letter(idx: int) -> str:
    """0-indexed column position -> spreadsheet column letter (0 -> 'A', 26 -> 'AA')."""
    letter = ""
    idx += 1
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letter = chr(65 + rem) + letter
    return letter


# Backwards-compatible alias for the shared 429 retry helper.
def with_retry(func, *args, max_retries=4, base_delay=15, **kwargs):
    """Retry wrapper with linear backoff for Google Sheets API 429 rate limits."""
    return with_retry_on_429(func, *args, max_retries=max_retries, delay=base_delay, **kwargs)


def get_df_minimal(
    sh: gspread.Spreadsheet,
    actual_sheet_name: str,
    tanggal_col: str,
    toko_col: str | None,
    required_cols: list[str] | None,
    header_row: int = 2,
    data_start_row: int = 3,
) -> pd.DataFrame:
    """Fetch ONLY the columns actually needed from a Google Sheet.

    tanggal_col / toko_col / required_cols must be passed as snake_case
    (matching how to_snake_case normalizes the real header).

    Uses a single batch_get call for all needed columns — one network round-trip.
    """
    ws = with_retry_on_429(sh.worksheet, actual_sheet_name)
    header_raw = with_retry(ws.row_values, header_row + 1)
    header_norm = [to_snake_case(c) for c in header_raw]

    needed = [tanggal_col] + ([toko_col] if toko_col else []) + list(required_cols or [])
    needed = list(dict.fromkeys(needed))  # dedupe, preserve order

    ranges = []
    for name in needed:
        if name not in header_norm:
            raise KeyError(f"column '{name}' not found. Available: {header_norm}")
        col_letter = col_index_to_letter(header_norm.index(name))
        ranges.append(f"{col_letter}{data_start_row + 1}:{col_letter}")

    results = with_retry(ws.batch_get, ranges, value_render_option="UNFORMATTED_VALUE")

    columns_data = {}
    for name, col_values in zip(needed, results):
        columns_data[name] = [row[0] if row else "" for row in col_values]

    max_len = max((len(v) for v in columns_data.values()), default=0)
    for name in columns_data:
        columns_data[name] += [""] * (max_len - len(columns_data[name]))

    return pd.DataFrame(columns_data)


def get_max_tanggal_by_toko(
    df: pd.DataFrame,
    toko_col: str | None = "toko",
    tanggal_col: str = "tanggal",
    required_cols: list[str] | None = None,
    max_allowed: pd.Timestamp | str | None = None,
) -> tuple[pd.Series, pd.Series]:
    """Group by toko (or overall if toko_col is None) and return max tanggal.

    Rows where required_cols are blank are excluded before computing the max.
    If max_allowed is given, rows with tanggal > max_allowed are ignored for the
    max (future-dated input should not count as real progress); they are counted
    separately via the returned n_future_rows series.

    Returns a tuple (max_by_toko, n_future_rows), each a Series keyed like
    {toko_value: value}.
    """
    tmp = df.copy()
    tmp[tanggal_col] = parse_mixed_dates(tmp[tanggal_col], return_date=False)

    if required_cols:
        for col in required_cols:
            if col not in tmp.columns:
                raise KeyError(f"required_cols column '{col}' not found. Available: {list(tmp.columns)}")
            tmp = tmp[tmp[col].astype(str).str.strip().replace({"nan": ""}) != ""]

    if max_allowed is not None:
        cutoff = max_allowed if isinstance(max_allowed, pd.Timestamp) else pd.Timestamp(max_allowed)
        tmp["_n_future"] = (tmp[tanggal_col] > cutoff).astype(int)
        n_future = (
            tmp.groupby(toko_col)["_n_future"].sum()
            if toko_col is not None
            else pd.Series({"_all": int(tmp["_n_future"].sum())})
        )
        tmp = tmp[tmp[tanggal_col] <= cutoff]
    else:
        n_future = pd.Series(dtype=int)

    if toko_col is None:
        max_out = pd.Series({"_all": tmp[tanggal_col].max()})
    else:
        max_out = tmp.groupby(toko_col)[tanggal_col].max()

    keys = set(max_out.index) | set(n_future.index)
    max_out = max_out.reindex(keys)
    n_future = n_future.reindex(keys).fillna(0)

    return max_out, n_future


def _open_spreadsheets(sheet_registry: dict) -> dict:
    """Open each unique spreadsheet once, keyed by env var name (e.g. 'SH_KEY_MATZ')."""
    gc = get_gspread_client()
    objects = {}
    for spreadsheet_key, _worksheet in sheet_registry.values():
        if spreadsheet_key not in objects:
            objects[spreadsheet_key] = with_retry_on_429(gc.open_by_key, os.getenv(spreadsheet_key))
    return objects


def get_watermark(bucket: str, watermarks_path: str, minio_client=None) -> list[dict]:
    """Fetch the per-sheet watermark records from MinIO as a list of dicts.

    Missing file -> [] (first run / no watermark yet).
    """
    if minio_client is None:
        minio_client, _ = get_minio_client()
    _, records = get_sheet_watermarks(minio_client, bucket, watermarks_path)
    return records


# ---------------------------------------------------------------------------
# Core engine
# ---------------------------------------------------------------------------

def compare_watermark_vs_sheet(
    logical_name: str,
    registry_keys: list[str],
    sheet_registry: dict,
    watermark_records: list[dict],
    spreadsheet_objects: dict | None = None,
    toko_col: str | None = "toko",
    tanggal_col: str = "tanggal",
    required_cols: list[str] | None = None,
    header_row: int = 2,
    data_start_row: int = 3,
    max_allowed: pd.Timestamp | str | None = None,
) -> pd.DataFrame:
    """Compare stored watermark against live GSheet state per sheet_name.

    For each registry key, fetches the relevant columns from the live sheet,
    computes the max tanggal per toko (or overall), and compares against
    the watermark's last_processed_date. If max_allowed is given, future-dated
    rows (tanggal > max_allowed) are excluded from the max and only counted in
    the n_future_rows column.

    Returns a DataFrame with columns:
        sheet_name, grain, logical_sheet, sheet_max_tanggal, n_future_rows,
        last_processed_date, is_behind, status
    """
    if spreadsheet_objects is None:
        spreadsheet_objects = _open_spreadsheets(sheet_registry)

    rows = []
    for registry_key in registry_keys:
        if registry_key not in sheet_registry:
            rows.append({
                "sheet_name": registry_key, "grain": None, "logical_sheet": logical_name,
                "sheet_max_tanggal": None, "n_future_rows": None, "last_processed_date": None,
                "is_behind": False, "status": "registry_key not found in sheet_registry",
            })
            continue

        spreadsheet_key, worksheet_name = sheet_registry[registry_key]
        sh = spreadsheet_objects.get(spreadsheet_key)
        if sh is None:
            rows.append({
                "sheet_name": registry_key, "grain": None, "logical_sheet": logical_name,
                "sheet_max_tanggal": None, "n_future_rows": None, "last_processed_date": None,
                "is_behind": False, "status": "spreadsheet object not found",
            })
            continue

        try:
            df = get_df_minimal(
                sh, worksheet_name,
                tanggal_col=tanggal_col,
                toko_col=toko_col,
                required_cols=required_cols,
                header_row=header_row,
                data_start_row=data_start_row,
            )
            max_by_toko, n_future_by_toko = get_max_tanggal_by_toko(
                df, toko_col, tanggal_col, required_cols, max_allowed
            )
        except Exception as e:
            rows.append({
                "sheet_name": registry_key, "grain": None, "logical_sheet": logical_name,
                "sheet_max_tanggal": None, "n_future_rows": None, "last_processed_date": None,
                "is_behind": False, "status": f"error: {e}",
            })
            continue

        wm_rows = [r for r in watermark_records if r.get("sheet_name") == registry_key]
        for wm_row in wm_rows:
            grain_val = wm_row.get("toko") if toko_col is not None else None
            lookup_key = grain_val if toko_col is not None else "_all"
            sheet_max = max_by_toko.get(lookup_key, pd.NaT)
            n_future = int(n_future_by_toko.get(lookup_key, 0))
            last_processed = pd.to_datetime(wm_row.get("last_processed_date"))
            rows.append({
                "sheet_name": registry_key,
                "grain": grain_val,
                "logical_sheet": logical_name,
                "sheet_max_tanggal": sheet_max,
                "n_future_rows": n_future,
                "last_processed_date": last_processed,
                "is_behind": bool(pd.notna(sheet_max) and sheet_max > last_processed),
                "status": (
                    "ok"
                    if pd.notna(sheet_max)
                    else ("future-dated rows only" if n_future > 0 else "no data found")
                ),
            })

    df_out = pd.DataFrame(rows)
    if "is_behind" not in df_out.columns:
        df_out["is_behind"] = pd.Series(dtype=bool)
    return df_out


# ---------------------------------------------------------------------------
# Project-specific wrapper
# ---------------------------------------------------------------------------

def pesanan_affiliasi_watermark_check() -> pd.DataFrame:
    """Check watermark drift for the Pesanan Affiliasi sheets (incl. temp sheets) using toko grain."""
    watermark_records = get_watermark(
        bucket="tiktok-dgs-pesanan-affiliasi-bronze",
        watermarks_path="watermarks/pesanan_affiliasi.json",
    )
    sheet_registry = {
        "matz": ("SH_KEY_MATZ", "Pesanan Affiliasi"),
        "ian":  ("SH_KEY_IAN",  "Pesanan Affiliasi"),
        "deni": ("SH_KEY_DENI", "Pesanan Affiliasi"),
        "riwa": ("SH_KEY_RIWA", "Pesanan Affiliasi"),
        "imam": ("SH_KEY_IMAM", "Pesanan Affiliasi"),
        "riwa_ajwa":  ("SH_KEY_RIWA", "Pesanan Affiliasi Ajwa Sementara"),
        "deni_etawa": ("SH_KEY_DENI", "Pesanan Affiliasi Etawaherb Sementara"),
    }
    registry_keys = list(sheet_registry.keys())
    return compare_watermark_vs_sheet(
        "Pesanan Affiliasi", registry_keys, sheet_registry, watermark_records,
        toko_col="toko", tanggal_col="tanggal",
        required_cols=["id_pesanan"],
        max_allowed=pd.Timestamp(datetime.now().date()),
    )