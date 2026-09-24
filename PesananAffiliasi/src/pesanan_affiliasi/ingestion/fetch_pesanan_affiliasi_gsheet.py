# src/pesanan_affiliasi/ingestion/fetch_pesanan_affiliasi_gsheet.py
import os

import gspread
import pandas as pd

from src.pesanan_affiliasi.utils.gsheet_client import with_retry_on_429

ID_COLS = ["ID Pesanan", "ID Produk", "ID Konten"]

SHEET_REGISTRY = {
    "matz": ("SH_KEY_MATZ", "Pesanan Affiliasi"),
    "ian": ("SH_KEY_IAN", "Pesanan Affiliasi"),
    "deni": ("SH_KEY_DENI", "Pesanan Affiliasi"),
    "riwa": ("SH_KEY_RIWA", "Pesanan Affiliasi"),
    "imam": ("SH_KEY_IMAM", "Pesanan Affiliasi"),
    "riwa_ajwa": ("SH_KEY_RIWA", "Pesanan Affiliasi Ajwa Sementara"),
    "deni_etawa": ("SH_KEY_DENI", "Pesanan Affiliasi Etawaherb Sementara"),
}


def _float_id_to_str(value) -> str:
    """Converts a raw (unformatted) ID cell to its full-precision integer
    string. float64 is only reliable to ~15-17 significant digits, so an
    extremely long ID may still drift in the last 1-2 digits — but this
    preserves everything the stored double actually carries, instead of
    the 6-sig-fig truncation get_all_values()'s display text gives you."""
    if value is None or value == "":
        return ""
    if isinstance(value, str):
        return value.strip()  # already text — leave as-is
    try:
        return f"{float(value):.0f}"
    except (TypeError, ValueError):
        return str(value)


def _fetch_id_columns_unformatted(ws, header_row: list, id_cols=ID_COLS) -> dict:
    """Re-fetches only id_cols at full precision. Row-aligned with
    values[3:], matching how the main DataFrame is built (3 header rows
    skipped)."""
    raw_matrix = with_retry_on_429(ws.get_values, value_render_option="UNFORMATTED_VALUE")
    data_rows = raw_matrix[3:]

    result = {}
    for col in id_cols:
        if col not in header_row:
            continue
        idx = header_row.index(col)
        result[col] = [
            _float_id_to_str(row[idx] if idx < len(row) else "")
            for row in data_rows
        ]
    return result


def standardize_format_columns(df_source: pd.DataFrame, target_columns: list):
    # ... unchanged ...
    df_aff = df_source.copy()
    rename_mapping = {'Jenis Akun': 'Jenis Creator'}
    df_aff = df_aff.rename(columns=rename_mapping)
    missing_cols = set(target_columns) - set(df_aff.columns)
    for col in missing_cols:
        df_aff[col] = None
    if 'Tanggal' in df_aff.columns:
        df_aff['Tanggal'] = pd.to_datetime(df_aff['Tanggal'], errors='coerce').dt.strftime('%Y/%m/%d')
    df_aff = df_aff.reindex(columns=target_columns)
    return df_aff


def fetch_tiktok_pesanan_affiliasi(gc: gspread.Client) -> pd.DataFrame:
    sheets = {}
    for sheet_name, (env_key, worksheet) in SHEET_REGISTRY.items():
        sh = with_retry_on_429(gc.open_by_key, os.getenv(env_key))
        ws = with_retry_on_429(sh.worksheet, worksheet)
        values = with_retry_on_429(ws.get_all_values)
        header_row = values[2]
        df_sheet = pd.DataFrame(values[3:], columns=header_row)

        # Overwrite ID columns with full-precision unformatted values —
        # everything else keeps using the formatted read above.
        id_values = _fetch_id_columns_unformatted(ws, header_row, id_cols=ID_COLS)
        for col, vals in id_values.items():
            if col in df_sheet.columns and len(vals) == len(df_sheet):
                df_sheet[col] = vals
            elif col in df_sheet.columns:
                print(f"[INGEST][{sheet_name}] WARNING: unformatted ID fetch "
                      f"row count mismatch for '{col}' ({len(vals)} vs {len(df_sheet)}), "
                      f"keeping formatted (possibly truncated) values")

        sheets[sheet_name] = df_sheet
        print(f"[INGEST] {sheet_name}: {len(df_sheet)} rows")

    # ... rest unchanged (standardize_format_columns, concat, etc.) ...
    target_columns = sheets["matz"].columns.to_list()
    for temp in ("riwa_ajwa", "deni_etawa"):
        sheets[temp] = standardize_format_columns(sheets[temp], target_columns)

    frames = []
    sheet_creds = {name: os.getenv(env) for name, (env, _ws) in SHEET_REGISTRY.items()}
    for sheet_name, df_sheet in sheets.items():
        df_sheet = df_sheet.loc[:, ~df_sheet.columns.duplicated()]
        df_sheet["creds"] = sheet_creds[sheet_name]
        df_sheet["sheet_name"] = sheet_name
        frames.append(df_sheet)

    tiktok_affiliate = pd.concat(frames, ignore_index=True)
    return tiktok_affiliate