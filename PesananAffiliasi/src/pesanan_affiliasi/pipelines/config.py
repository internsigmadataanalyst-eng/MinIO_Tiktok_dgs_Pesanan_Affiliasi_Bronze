# src/pesanan_affiliasi/pipelines/config.py
"""Shared constants and mutable pipeline state for the Pesanan Affiliasi ETL."""

import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "database-sigma"
WATERMARK_PATH = "watermarks/pesanan_affiliasi.json"

SRC_GSHEET = {"system": "Google_Sheets", "entity": "Pesanan Affiliasi"}
SRC_MINIO = {"system": "MinIO", "entity": WATERMARK_PATH}
TGT_MINIO = {"system": "MinIO", "entity": "pesanan/affiliasi"}
TGT_MINIO_QUARANTINE = {"system": "MinIO", "entity": "quarantine"}
TGT_BQ_BRONZE = {"system": "BigQuery", "entity": f"{PROJECT_ID}.BRONZE_DB.bronze_affiliate"}
TGT_BQ_SILVER = {"system": "BigQuery", "entity": f"{PROJECT_ID}.SILVER_DB.silver_tt_affiliate"}

WHITELIST_SHEETS = set(
    s.strip() for s in os.getenv("WHITELIST_SHEETS", "deni_etawa,riwa_ajwa,ian").split(",") if s.strip()
)

BQ_TARGETS = [
    {"table": TGT_BQ_BRONZE["entity"], "action": "append"},
    {"table": TGT_BQ_SILVER["entity"], "action": "MERGE (silver upsert)"},
]

_failure_ctx = {
    "stage": "",
    "minio_files": [],
    "rollback_hint": "",
}
