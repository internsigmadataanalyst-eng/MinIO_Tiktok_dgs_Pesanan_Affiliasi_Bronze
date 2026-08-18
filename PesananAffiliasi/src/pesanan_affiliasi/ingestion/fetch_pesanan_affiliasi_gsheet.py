# src/pesanan_affiliasi/ingestion/fetch_pesanan_affiliasi_gsheet.py
import os

import gspread
import pandas as pd


# tag -> (env key GSheet, nama worksheet). Setiap aliran data punya tag UNIK:
# worksheet sementara (riwa_ajwa & deni_etawa) adalah aliran terpisah meskipun
# datang dari GSheet yang sama dengan sheet utama riwa/deni.
SHEET_REGISTRY = {
    "matz": ("SH_KEY_MATZ", "Pesanan Affiliasi"),
    "ian": ("SH_KEY_IAN", "Pesanan Affiliasi"),
    "deni": ("SH_KEY_DENI", "Pesanan Affiliasi"),
    "riwa": ("SH_KEY_RIWA", "Pesanan Affiliasi"),
    "imam": ("SH_KEY_IMAM", "Pesanan Affiliasi"),
    "riwa_ajwa": ("SH_KEY_RIWA", "Pesanan Affiliasi Ajwa Sementara"),
    "deni_etawa": ("SH_KEY_DENI", "Pesanan Affiliasi Etawaherb Sementara"),
}


def standardize_format_columns(df_source: pd.DataFrame, target_columns: list):
    df_aff = df_source.copy()

    # 1. Rename kolom yang beda nama tapi secara fungsi sama
    rename_mapping = {
        'Jenis Akun': 'Jenis Creator'
    }
    df_aff = df_aff.rename(columns=rename_mapping)

    # 2. Tambahkan kolom yang tidak ada di sheet utama dengan nilai default/null
    missing_cols = set(target_columns) - set(df_aff.columns)
    for col in missing_cols:
        df_aff[col] = None  # atau pd.NA

    # 3. Format tanggal agar seragam (YYYY/MM/DD)
    if 'Tanggal' in df_aff.columns:
        df_aff['Tanggal'] = pd.to_datetime(df_aff['Tanggal'], errors='coerce').dt.strftime('%Y/%m/%d')

    # 4. Reindex kolom sesuai urutan persis sheet utama
    df_aff = df_aff.reindex(columns=target_columns)

    return df_aff


def fetch_tiktok_pesanan_affiliasi(gc: gspread.Client) -> pd.DataFrame:
    """
    Ambil data pesanan affiliasi dari SHEET_REGISTRY,
    tag tiap sheet dengan kolom 'sheet_name', lalu concat jadi satu
    DataFrame raw (belum dibersihkan).
    """
    sheets = {}
    for sheet_name, (env_key, worksheet) in SHEET_REGISTRY.items():
        sh = gc.open_by_key(os.getenv(env_key))
        ws = sh.worksheet(worksheet)
        values = ws.get_all_values()
        sheets[sheet_name] = pd.DataFrame(values[3:], columns=values[2])
        print(f"[INGEST] {sheet_name}: {len(sheets[sheet_name])} rows")

    # Special Handling worksheet sementara agar kolomnya seragam dengan sheet utama
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
