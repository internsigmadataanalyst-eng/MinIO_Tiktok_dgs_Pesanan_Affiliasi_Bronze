# src/pesanan_affiliasi/pipelines/run_daily_etl.py

import os
import io
from datetime import date
from google.oauth2 import service_account

from dotenv import load_dotenv

# Load variables from .env into environment
load_dotenv()

from src.pesanan_affiliasi.utils.gsheet_client import get_gspread_client
from src.pesanan_affiliasi.utils.bq_client import get_bq_client
from src.pesanan_affiliasi.utils.minio_client import (
    get_minio_client, 
    get_minio_watermark, 
    update_minio_watermark
)
from src.pesanan_affiliasi.ingestion.fetch_pesanan_affiliasi_gsheet import (
    fetch_tiktok_pesanan_affiliasi,
)
from src.pesanan_affiliasi.transform.clean_bronze import build_bronze_affiliate
from src.pesanan_affiliasi.transform.merge_silver import merge_to_silver
from src.pesanan_affiliasi.transform.build_gold import build_fact_affiliate
from src.pesanan_affiliasi.load.load_to_bigquery import load_df

PROJECT_ID = "database-sigma"


def _get_credentials():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError("Env GOOGLE_APPLICATION_CREDENTIALS belum di-set")
    return service_account.Credentials.from_service_account_file(sa_path)


def run_daily_etl():
    print("== Start ETL Pesanan Affiliasi ==")

    # 1) Client
    gc = get_gspread_client()
    bq_client = get_bq_client()
    creds = _get_credentials()

    # 2) Ingest dari GSheet
    df_raw = fetch_tiktok_pesanan_affiliasi(gc)
    print(f"[INGEST] Rows raw from GSheet: {len(df_raw)}")

    # 3) Bronze: cleaning + snapshot + hash
    df_bronze, _ = build_bronze_affiliate(df_raw)
    print(f"[BRONZE] Rows bronze to load: {len(df_bronze)}")

    load_df(
        df_bronze,
        table_id="BRONZE_DB.bronze_affiliate",
        project_id=PROJECT_ID,
        if_exists="append",
        credentials=creds,
    )
    print("[BRONZE] Load to BRONZE_DB.bronze_affiliate DONE")

    # 4) Silver: MERGE
    print("[SILVER] Running MERGE into SILVER_DB.silver_tt_affiliate ...")
    merge_to_silver()
    print("[SILVER] MERGE DONE")

    # 5) Gold: fact_live_performa_daily
    print("[GOLD] Building fact_live_performa_daily ...")
    df_fact = build_fact_affiliate(bq_client)
    print(f"[GOLD] Rows fact_live_performa_daily: {len(df_fact)}")

    load_df(
        df_fact,
        table_id="GOLD_DB.fact_tt_affiliate",
        project_id=PROJECT_ID,
        if_exists="replace",  # nanti bisa jadi MERGE kalau mau incremental
        credentials=creds,
    )
    print("[GOLD] Load to GOLD_DB.fact_tt_affiliate DONE")

    print("== ETL Pesanan Affiliasi DONE ==")

# Kalau kamu mau bisa juga di-run langsung:
if __name__ == "__main__":
    run_daily_etl()