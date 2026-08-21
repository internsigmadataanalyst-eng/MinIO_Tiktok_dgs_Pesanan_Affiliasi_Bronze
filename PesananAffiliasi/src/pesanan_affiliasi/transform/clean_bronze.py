# src/pesanan_affiliasi/transform/clean_bronze.py
import uuid
import hashlib
from datetime import datetime, timezone

import pandas as pd

from src.pesanan_affiliasi.utils.transform_utils import (
    # clean_numeric_columns,
    parse_mixed_dates,
    to_snake_case,
)
from src.pesanan_affiliasi.utils.minio_client import filter_by_sheet_watermark


# NUMERIC_COLS = [
#     "Harga",
#     "Payment Amount",
#     "Kuantitas",
#     "Persentase komisi standar",
#     "Est. Acuan Komisi",
#     "Perkiraan pembayaran komisi standar",
#     "Acuan Komisi Aktual",
#     "Pembayaran Komisi Aktual",
#     "Persentase komisi Iklan Belanja",
#     "Perkiraan pembayaran komisi Iklan Belanja",
#     "Pembayaran komisi Iklan Belanja aktual",
#     "Perkiraan bonus yang ditanggung bersama untuk kreator",
#     "Bonus sebenarnya yang ditanggung bersama untuk kreator",
#     "Pengembalian barang",
#     "Pengembalian dana",
# ]


def _canon(x):
    import pandas as pd

    x = "" if pd.isna(x) else str(x).strip()
    return x.upper()


def build_bronze_affiliate(
    tiktok_pesanan_raw: pd.DataFrame, sheet_watermarks: dict | None = None
) -> tuple[pd.DataFrame, dict]:
    """
    Dari raw GSheet → cleaning numeric + tanggal + snake_case,
    tambah snapshot_ts, snapshot_date, run_id, row_hash_raw.
    Filter incremental per sheet_name berdasarkan watermark (sheet_watermarks).
    Output: (df siap di-load ke BRONZE_DB.bronze_live, sheet_max_dates)
    """
    # numeric cleaning
    # tiktok_affiliate_clean1 = clean_numeric_columns(
    #     tiktok_pesanan_raw, NUMERIC_COLS, fillna_value=0
    # )

    tiktok_affiliate_clean1 = tiktok_pesanan_raw.copy()
    # parse tanggal
    tiktok_affiliate_clean1["Waktu Dibuat"] = parse_mixed_dates(
        tiktok_affiliate_clean1["Waktu Dibuat"], return_date=False
    )
    tiktok_affiliate_clean1["Tanggal"] = parse_mixed_dates(
        tiktok_affiliate_clean1["Tanggal"], return_date=False
    )
    tiktok_affiliate_clean1["Waktu Pembayaran"] = parse_mixed_dates(
        tiktok_affiliate_clean1["Waktu Pembayaran"], return_date=False
    )
    tiktok_affiliate_clean1["Waktu Pesanan Siap Dikirim"] = parse_mixed_dates(
        tiktok_affiliate_clean1["Waktu Pesanan Siap Dikirim"], return_date=False
    )
    tiktok_affiliate_clean1["Waktu Pesanan Selesai"] = parse_mixed_dates(
        tiktok_affiliate_clean1["Waktu Pesanan Selesai"], return_date=False
    )
    tiktok_affiliate_clean1["Waktu Komisi Dibayar"] = parse_mixed_dates(
        tiktok_affiliate_clean1["Waktu Komisi Dibayar"], return_date=False
    )
    tiktok_affiliate_clean1["Order Delivery Time"] = parse_mixed_dates(
        tiktok_affiliate_clean1["Order Delivery Time"], return_date=False
    )

    # copy & snake_case
    df = tiktok_affiliate_clean1.copy()
    df.columns = df.columns.map(to_snake_case)

    # buang baris tanpa id
    df = df[df["id_pesanan"].astype(str).str.strip() != ""]

    # tidak ada data valid → biarkan pipeline memberi tahu "up-to-date"
    if df.empty:
        return df, {}

    # snapshot fields
    now_utc = datetime.now(timezone.utc)
    df["snapshot_ts"] = now_utc
    df["snapshot_date"] = now_utc.date()
    df["run_id"] = str(uuid.uuid4())

    # row_hash_raw: sesuai scriptmu
    cols_for_hash = ["waktu_dibuat","toko","id_pesanan","waktu_pembayaran","waktu_pesanan_siap_dikirim","status_pesanan"]

    df["row_hash_raw"] = (
        df[cols_for_hash]
        .map(_canon)
        .astype(str)
        .agg("||".join, axis=1)
        .apply(lambda s: hashlib.sha256(s.encode()).hexdigest())
    )

    # buang kolom dengan nama kosong/whitespace
    df = df.loc[:, df.columns.str.strip().astype(bool)]

    # buang kolom dengan data spesifik
    cols_to_drop = ['open__target_collaboration', 'jenis_akun', 'jenis_creator']
    df = df.drop(columns=cols_to_drop, errors='ignore')

    # Filter incremental per sheet (sheet_name-keyed) berdasarkan watermark.
    # Tiap sheet punya watermark sendiri, termasuk yang berbagi creds
    # (riwa & riwa_ajwa, deni & deni_etawa) agar tidak saling memblokir.
    if "sheet_name" in df.columns:
        df, sheet_max_dates = filter_by_sheet_watermark(
            df, "sheet_name", "tanggal", sheet_watermarks or {}
        )
    else:
        sheet_max_dates = {}

    # NOTE: creds & sheet_name sengaja DIPERTAHANKAN di level bronze.
    return df, sheet_max_dates