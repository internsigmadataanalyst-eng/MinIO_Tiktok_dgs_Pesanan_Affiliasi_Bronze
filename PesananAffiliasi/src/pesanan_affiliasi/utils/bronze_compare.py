# src/pesanan_affiliasi/utils/bronze_compare.py
"""Watermark display and comparison utilities."""

import pandas as pd


def effective_watermark_changes(
    watermark_records: list, sheet_max_dates: dict
) -> dict:
    """Returns only the (creds, sheet_name, toko) -> max_date entries whose
    candidate max_date actually DIFFERS from the currently-stored watermark.

    Groups with no stored watermark yet (new groups) are included.
    """
    stored = {}
    for rec in watermark_records or []:
        creds = str(rec.get("creds") or "")
        sheet_name = str(rec.get("sheet_name") or "")
        toko = str(rec.get("toko") or "")
        date_val = str(rec.get("last_processed_date") or rec.get("last_update") or "").strip()[:10]
        stored[(creds, sheet_name, toko)] = date_val

    changes = {}
    for (creds, sheet_name, toko), max_date in sheet_max_dates.items():
        key = (str(creds), str(sheet_name or ""), str(toko or ""))
        if max(stored.get(key, ""), "").strip() == str(max_date).strip()[:10]:
            continue
        changes[key] = str(max_date)
    return changes


def show_watermark(watermark_records: list):
    """Show the current MinIO watermark."""
    if not watermark_records:
        print("[WATERMARK] (tidak ada watermark MinIO yang ditemukan)")
        return
    print("\n[WATERMARK] Current watermark (MinIO):")
    wm_df = pd.DataFrame(watermark_records)[
        ["creds", "sheet_name", "toko", "last_processed_date"]
    ].sort_values(["creds", "sheet_name", "toko"])
    print(wm_df.to_string(index=False))


def fmt_drift_date(val) -> str:
    """Format a drift-check date value for the gate-2 email table ('' for NaT)."""
    try:
        if val is None or pd.isna(val):
            return ""
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        return str(val)
    except Exception:
        return str(val or "")


def build_drift_rows(status_df: pd.DataFrame) -> list[dict]:
    """Per-sheet watermark drift summary from the pre-flight check."""
    rows = []
    for _, row in status_df.iterrows():
        rows.append({
            "sheet_name": str(row.get("sheet_name") or ""),
            "toko": str(row.get("grain") or ""),
            "gsheet_max": fmt_drift_date(row.get("sheet_max_tanggal")),
            "watermark": fmt_drift_date(row.get("last_processed_date")),
            "status": "BEHIND" if row.get("is_behind") else "ok",
        })
    rows.sort(key=lambda r: (r["sheet_name"], r["toko"]))
    return rows
