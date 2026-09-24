# src/pesanan_affiliasi/pipelines/run_daily_etl.py

import io
import os
import traceback
from datetime import datetime

import pandas as pd

from src.pesanan_affiliasi.pipelines.config import (
    PROJECT_ID,
    WATERMARK_PATH,
    SRC_GSHEET,
    SRC_MINIO,
    TGT_MINIO,
    TGT_MINIO_QUARANTINE,
    TGT_BQ_BRONZE,
    TGT_BQ_SILVER,
    WHITELIST_SHEETS,
    BQ_TARGETS,
    _failure_ctx,
)
from src.pesanan_affiliasi.utils.gsheet_client import get_gspread_client
from src.pesanan_affiliasi.utils.minio_client import (
    get_minio_client,
    get_sheet_watermarks,
    update_sheet_watermarks,
    write_quarantine,
    sync_error_manifest,
    filter_already_quarantined,
    QUARANTINE_PREFIX,
)
from src.pesanan_affiliasi.utils.transform_utils import (
    NUMERIC_COLS,
    PERCENT_COLS,
    validate_and_normalize_raw,
    cap_watermark_advance_at_low_volume,
)
from src.pesanan_affiliasi.ingestion.fetch_pesanan_affiliasi_gsheet import (
    fetch_tiktok_pesanan_affiliasi,
    SHEET_REGISTRY,
)
from src.pesanan_affiliasi.transform.clean_bronze import build_bronze_affiliate
from src.pesanan_affiliasi.transform.merge_silver import merge_to_silver
from src.pesanan_affiliasi.load.load_to_bigquery import load_df
from src.pesanan_affiliasi.utils.log import (
    get_log_folder,
    write_section_log,
    setup_event_logging,
    is_event_logging_enabled,
    emit,
    write_wm_log,
    write_failure_log,
)
from src.pesanan_affiliasi.utils.watermark_monitor import pesanan_affiliasi_watermark_check
from src.pesanan_affiliasi.utils.notify import (
    send_alert_email,
    build_gate_abort_email,
    build_quarantine_email,
    build_recovery_email,
    QUARANTINE_SAMPLE_ROWS,
    QUARANTINE_SAMPLE_COLUMNS,
    bq_full_load,
    bq_noop,
    finish,
)
from src.pesanan_affiliasi.utils.bronze_compare import (
    effective_watermark_changes,
    build_drift_rows,
)
from src.pesanan_affiliasi.utils.bq_client import (
    get_credentials,
    fetch_existing_bronze_hashes,
)
from src.pesanan_affiliasi.utils.recovery import select_recovered


def run_daily_etl(dry_run: bool | None = None):
    print("== Start ETL Pesanan Affiliasi ==")

    if dry_run is None:
        dry_run = os.getenv("ETL_DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y"}

    if dry_run:
        print("[DRY-RUN] Mode aktif: TIDAK ada data yang ditulis ke MinIO/BigQuery/Silver.")

    _failure_ctx.clear()
    _failure_ctx.update({
        "stage": "", "minio_files": [], "rollback_hint": "",
    })

    gc = get_gspread_client()
    creds = get_credentials()
    minio_client, minio_bucket = get_minio_client()

    now_obj = datetime.now()
    today_key = now_obj.strftime("%Y%m%d")
    run_key = now_obj.strftime("%Y%m%d%H%M")
    log_folder = get_log_folder(run_key) if not dry_run else None

    if not is_event_logging_enabled():
        _, stop_event_logging = setup_event_logging(run_key, dry_run)
    else:
        stop_event_logging = None
    emit("PIPELINE", "run_daily_etl", "ETL start",
         metrics={"dry_run": dry_run},
         source=SRC_GSHEET, target=TGT_BQ_SILVER)

    print("\n" + "=" * 70)
    print("--- PRE-FLIGHT: Watermark Drift Check ---")
    print("=" * 70)
    status_df = pesanan_affiliasi_watermark_check()

    view = pd.DataFrame({
        "sheet_name": status_df["sheet_name"],
        "toko": status_df["grain"],
        "gsheet": status_df["sheet_max_tanggal"],
        "wm": status_df["last_processed_date"],
        "future": status_df.get("n_future_rows", pd.Series(0, index=status_df.index)),
        "flag": status_df["is_behind"].map({True: "BEHIND", False: "ok"}),
    })
    print("-" * 70)
    print("DATASET: PESANAN AFFILIASI (toko grain)")
    print("-" * 70)
    print(view.sort_values(["sheet_name", "toko"]).to_string(
        index=False,
        formatters={
            "gsheet": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
            "wm": lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) else "",
            "future": lambda v: "" if pd.isna(v) else str(int(v)),
        },
    ))
    print()

    total_sheets = status_df["sheet_name"].nunique() if len(status_df) else 0
    n_behind = int(status_df["is_behind"].sum()) if len(status_df) else 0
    emit("PRE_FLIGHT", "watermark_monitor", "Watermark drift check complete",
         metrics={"total_groups": total_sheets,
                  "groups_behind": status_df.groupby("sheet_name")["is_behind"].any().sum() if len(status_df) else 0,
                  "n_sheets": total_sheets, "n_behind": n_behind},
         source=SRC_MINIO)

    has_errors = bool(len(status_df)) and status_df["status"].str.startswith("error").any()
    if has_errors:
        error_sheets = status_df[status_df["status"].str.startswith("error")]["sheet_name"].unique().tolist()
        mode = "DRY-RUN WOULD ABORT" if dry_run else "ABORT"
        print(f"[GATE] {mode} - access errors on sheets: {error_sheets}")
        print("[GATE] Fix the sheet access issue and re-run.")
        emit("GATE", "watermark_monitor",
             f"{mode} - access errors on sheets: {error_sheets}",
             level="ERROR",
             metrics={"error_sheets": error_sheets},
             source=SRC_MINIO)
        subject, body_html = build_gate_abort_email(
            gate="1",
            mode=mode,
            message=f"Access errors while checking sheets: {error_sheets}",
            lists={"error_sheets": error_sheets},
            note="Fix the sheet access issue and re-run.",
            bq_updates=BQ_TARGETS,
            enable_explanation=not dry_run,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)
        if log_folder:
            write_wm_log(log_folder, run_key, status_df, pd.Series(dtype=bool),
                         f"ABORT - access errors on sheets: {error_sheets}")
            write_failure_log(
                log_folder, run_key,
                f"access errors on sheets: {error_sheets}",
            )
        return

    sheet_passes = status_df.groupby("sheet_name")["is_behind"].any()
    caught_up = [s for s in sheet_passes[~sheet_passes].index.tolist() if s not in WHITELIST_SHEETS]
    behind_sheets = sheet_passes[sheet_passes].index.tolist()
    whitelisted_skipped = [s for s in sheet_passes[~sheet_passes].index.tolist() if s in WHITELIST_SHEETS]

    if caught_up:
        mode = "DRY-RUN WOULD ABORT" if dry_run else "ABORT"
        print(f"[GATE] Sheets already up-to-date (skipped): {caught_up}")
        print(f"[GATE] Sheets with new data: {behind_sheets}")
        if whitelisted_skipped:
            print(f"[GATE] Whitelisted sheets : {whitelisted_skipped}")
        print(f"[GATE] {mode} - sheets with no new data are required before continuing.")

        today_ts = pd.Timestamp(datetime.now().date())
        suspicious = []
        for sheet_name in caught_up:
            sel = status_df[status_df["sheet_name"] == sheet_name]
            for _, row in sel[~sel["is_behind"]].iterrows():
                wm = row["last_processed_date"]
                sm = row["sheet_max_tanggal"]
                if pd.notna(wm) and pd.notna(sm) and wm > sm:
                    suspicious.append({
                        "sheet_name": sheet_name,
                        "grain": row["grain"],
                        "watermark": wm,
                        "sheet_max": sm,
                        "in_future": bool(wm > today_ts),
                    })
        if suspicious:
            print("[GATE] NOTE: some caught-up tokos have a watermark AHEAD of their live sheet max:")
            for s in suspicious:
                flag = " (watermark date is in the FUTURE)" if s["in_future"] else ""
                print(
                    f"[GATE]   {s['sheet_name']}/{s['grain']}: "
                    f"watermark {s['watermark']:%Y-%m-%d} vs sheet max {s['sheet_max']:%Y-%m-%d}{flag}"
                )
            if any(s["in_future"] for s in suspicious):
                print("[GATE]   Fix the future-dated input upstream, then manually reset")
                print("[GATE]     the stray watermark date to the last real date.")

        toko_detail = []
        for sheet_name in caught_up:
            sel = status_df[status_df["sheet_name"] == sheet_name]
            toko_without_new = sel[~sel["is_behind"]]["grain"].tolist()
            toko_detail.append({"sheet_name": sheet_name,
                                "toko_without_new_data": toko_without_new})

        emit(
            "GATE", "etl_gate",
            f"Sheets with no new data are required before continuing: {caught_up}",
            level="ERROR",
            metrics={
                "caught_up": caught_up,
                "behind": behind_sheets,
                "whitelisted_skipped": whitelisted_skipped,
                "caught_up_detail": toko_detail,
                "caught_up_suspicious": [
                    {**s, "watermark": s["watermark"].strftime("%Y-%m-%d"),
                     "sheet_max": s["sheet_max"].strftime("%Y-%m-%d")}
                    for s in suspicious
                ],
                "total_groups": int(len(status_df)),
                "sheets_up_to_date": len(caught_up),
                "sheets_with_new_data": len(behind_sheets),
            },
            source=SRC_GSHEET,
            target=SRC_MINIO,
        )
        subject, body_html = build_gate_abort_email(
            gate="2",
            mode=mode,
            message=(
                "Sheets with no new data are required before continuing: "
                f"{caught_up}"
            ),
            lists={
                "sheets_up_to_date": caught_up,
                "behind_sheets": behind_sheets,
                "whitelisted_skipped": whitelisted_skipped,
            },
            note=(
                "Fix the future-dated input upstream, then manually reset "
                "the stray watermark date to the last real date."
                if any(s["in_future"] for s in suspicious)
                else ""
            ),
            bq_updates=BQ_TARGETS,
            drift_rows=build_drift_rows(status_df),
            enable_explanation=not dry_run,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)
        if log_folder:
            write_wm_log(log_folder, run_key, status_df, sheet_passes,
                         f"ABORT - sheets with no new data: {caught_up}")
            write_failure_log(log_folder, run_key,
                              f"sheets with no new data (already up-to-date): {caught_up}")
        return

    print("--- PRE-FLIGHT PASSED ---\n")

    if log_folder:
        pass_count = int(sheet_passes.sum())
        total = len(sheet_passes)
        verdict = f"PASS - {pass_count}/{total} sheets have >=1 toko behind"
        write_wm_log(log_folder, run_key, status_df, sheet_passes, verdict)

    sheet_registry = {name: os.getenv(env_key) for name, (env_key, _ws) in SHEET_REGISTRY.items()}
    watermark_map, watermark_records = get_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, sheet_registry=sheet_registry
    )

    df_raw = fetch_tiktok_pesanan_affiliasi(gc)
    print(f"[INGEST] Rows raw from GSheet: {len(df_raw)}")
    emit("INGEST", "gsheet_ingester", f"Fetched {len(df_raw)} raw rows from GSheet",
         metrics={"rows_raw": int(len(df_raw))},
         source=SRC_GSHEET)

    df_raw = df_raw[df_raw["ID Pesanan"].astype(str).str.strip() != ""]
    df_valid, df_error, v_report = validate_and_normalize_raw(
        df_raw, NUMERIC_COLS, percent_cols=PERCENT_COLS
    )
    print(
        f"[VALIDATE] Rows valid: {len(df_valid)} | bad rows: {v_report['n_bad_rows']} "
        f"(date errors: {v_report['n_date_errors']} | future date errors: {v_report.get('n_date_future',0)} | toko_blank: {v_report.get('n_toko_blank',0)} | id errors: {v_report.get('n_id_errors',0)}) | blank rows dropped: {v_report['n_blank_rows']}"
    )
    emit(
        "VALIDATE", "validator",
        f"Validated: {len(df_valid)} valid, {v_report['n_bad_rows']} bad",
        level="WARN" if v_report["n_bad_rows"] else "INFO",
        metrics={
            "rows_raw": int(len(df_raw)),
            "rows_valid": int(len(df_valid)),
            "n_bad_rows": int(v_report["n_bad_rows"]),
            "n_date_errors": int(v_report["n_date_errors"]),
            "n_date_future": int(v_report.get("n_date_future", 0)),
            "n_toko_blank": int(v_report.get("n_toko_blank", 0)),
            "n_id_errors": int(v_report.get("n_id_errors", 0)),
            "n_blank_rows": int(v_report["n_blank_rows"]),
        },
        source=SRC_GSHEET,
        target=TGT_MINIO_QUARANTINE,
    )
    if v_report["has_changes"]:
        print(f"[VALIDATE] Corrupted/Shifted columns: {v_report['affected_columns']}")
        print(
            f"[VALIDATE] Affected date range: {v_report['first_affected_date']} "
            f"---> {v_report['last_affected_date']}"
        )

    df_error_new = (
        filter_already_quarantined(minio_client, minio_bucket, df_error)
        if not df_error.empty
        else df_error
    )

    resolved = sync_error_manifest(minio_client, minio_bucket, df_error, v_report, today_key, run_key, df_valid=df_valid, dry_run=dry_run)

    n_dupes_skipped = max(0, len(df_error) - len(df_error_new))
    src_rows = df_error_new if not df_error_new.empty else df_error
    reason_counts = {}
    affected_cols = []
    if not src_rows.empty and "error_reason" in src_rows.columns:
        reason_counts = (
            src_rows["error_reason"].str.split("|")
            .explode().value_counts().to_dict()
        )
        affected_from_dates = set()
        for cols in src_rows["error_reason"].str.findall(r"date_unparsable\((\w+)="):
            affected_from_dates.update(cols)
        affected_cols = sorted(
            affected_from_dates | set(v_report.get("affected_columns", []))
        )
    elif not src_rows.empty:
        affected_cols = sorted(set(v_report.get("affected_columns", [])))

    if not df_error_new.empty:
        if dry_run:
            print(f"[DRY-RUN] Akan quarantine {len(df_error_new)} bad row(s)")
        else:
            write_quarantine(minio_client, minio_bucket, df_error_new, today_key, run_key)

            if log_folder:
                import re as _re
                q_lines = []
                q_lines.append(f"=== QUARANTINE REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
                q_lines.append("Dataset: pesanan_affiliasi")
                q_lines.append(f"  Total quarantined rows: {len(df_error_new)}\n")

                if reason_counts:
                    q_lines.append("  Error reasons breakdown:")
                    for reason, count in reason_counts.items():
                        q_lines.append(f"    {reason} : {count} rows")
                    q_lines.append("")

                if affected_cols:
                    q_lines.append(f"  Affected columns: {affected_cols}\n")

                sample = df_error_new.head(5)
                q_lines.append(f"  Sample bad rows (first {len(sample)}):")
                display_cols = [c for c in ["Tanggal", "tanggal", "Toko", "toko",
                                             "ID Pesanan", "id_pesanan", "error_reason"] if c in sample.columns]
                if display_cols:
                    header = " | ".join(f"{c:<15}" for c in display_cols)
                    q_lines.append(f"    | {header} |")
                    q_lines.append(f"    | {'-' * len(header)} |")
                    for _, r in sample.iterrows():
                        vals = " | ".join(f"{str(r.get(c, '')):<15}" for c in display_cols)
                        q_lines.append(f"    | {vals} |")

                q_lines.append("")
                write_section_log(log_folder, f"quarantine_errors_{run_key}.log", "\n".join(q_lines) + "\n")

    if not df_error.empty:
        emit(
            "QUARANTINE", "validator",
            f"{len(df_error_new)} bad row(s) quarantined (new) | "
            f"{n_dupes_skipped} already-known duplicate(s) skipped",
            level="WARN",
            metrics={
                "n_quarantined": int(len(df_error_new)),
                "n_detected": int(len(df_error)),
                "n_duplicates_skipped": int(n_dupes_skipped),
            },
            source=SRC_GSHEET,
            target=TGT_MINIO_QUARANTINE,
        )
        subject, body_html = build_quarantine_email(
            len(df_error),
            reason_counts,
            affected_cols,
            sample_rows=[
                {c: r[c] for c in QUARANTINE_SAMPLE_COLUMNS if c in src_rows.columns}
                for _, r in src_rows.head(QUARANTINE_SAMPLE_ROWS).iterrows()
            ],
            n_duplicates_skipped=int(n_dupes_skipped),
            minio_path=(
                f"{QUARANTINE_PREFIX}/date={today_key}/quarantine_{run_key}.parquet"
                if not dry_run and not df_error_new.empty
                else ""
            ),
            log_path=(
                os.path.join(log_folder, f"quarantine_errors_{run_key}.log")
                if log_folder
                else ""
            ),
            bq_updates=BQ_TARGETS,
            enable_explanation=not dry_run,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)

    df_recovered = select_recovered(df_valid, resolved, v_report)
    print(
        f"[RECOVERY] resolved={v_report.get('recovery_resolved', 0)} "
        f"| recovered_rows={v_report.get('recovery_recovered_rows', 0)} "
        f"| absent={v_report.get('recovery_absent', 0)} "
        f"| count_mismatch_skipped={v_report.get('recovery_count_mismatch', 0)}"
    )
    emit(
        "RECOVERY", "error_recovery",
        f"Recovery: resolved={v_report.get('recovery_resolved', 0)}, "
        f"recovered_rows={v_report.get('recovery_recovered_rows', 0)}, "
        f"absent={v_report.get('recovery_absent', 0)}, "
        f"count_mismatch_skipped={v_report.get('recovery_count_mismatch', 0)}",
        level="WARN" if v_report.get("recovery_count_mismatch", 0) or v_report.get("recovery_absent", 0) else "INFO",
        metrics={
            "resolved": int(v_report.get("recovery_resolved", 0)),
            "recovered_rows": int(v_report.get("recovery_recovered_rows", 0)),
            "absent": int(v_report.get("recovery_absent", 0)),
            "count_mismatch_skipped": int(v_report.get("recovery_count_mismatch", 0)),
        },
        source=SRC_GSHEET,
        target=TGT_BQ_BRONZE,
    )
    if v_report.get("recovery_recovered_rows", 0) or v_report.get("recovery_resolved", 0):
        subject, body_html = build_recovery_email(
            resolved=v_report.get("recovery_resolved", 0),
            recovered_rows=v_report.get("recovery_recovered_rows", 0),
            absent=v_report.get("recovery_absent", 0),
            count_mismatch_skipped=v_report.get("recovery_count_mismatch", 0),
            bq_updates=BQ_TARGETS,
            enable_explanation=not dry_run,
        )
        send_alert_email(subject, body_html, dry_run=dry_run)

    df_regular = df_valid.drop(df_recovered.index)
    df_bronze_regular, sheet_max_dates = build_bronze_affiliate(
        df_regular, sheet_watermarks=watermark_map
    )

    if df_recovered.empty:
        df_bronze_recovered = df_bronze_regular.iloc[0:0]
    else:
        df_bronze_recovered, recovered_max_dates = build_bronze_affiliate(
            df_recovered, sheet_watermarks={}
        )
        for key, max_date in recovered_max_dates.items():
            sheet_max_dates[key] = max(sheet_max_dates.get(key, max_date), max_date)

    df_bronze = pd.concat(
        [df_bronze_regular, df_bronze_recovered], ignore_index=True
    ).drop_duplicates(subset=["row_hash_raw"])

    future_grains = set()
    if not df_error.empty and "error_reason" in df_error.columns:
        fm = df_error["error_reason"].astype(str).str.contains(
            "date_future", na=False
        )
        sn_col = "sheet_name" if "sheet_name" in df_error.columns else "creds"
        toko_col = (
            "Toko" if "Toko" in df_error.columns
            else ("toko" if "toko" in df_error.columns else None)
        )
        for _, _r in df_error.loc[fm].iterrows():
            future_grains.add(
                (str(_r[sn_col]), "" if toko_col is None else str(_r[toko_col]))
            )
    if future_grains:
        print(
            "[WATERMARK] low-volume cap hanya untuk suspect sheet-toko: "
            f"{sorted(future_grains)}"
        )
    sheet_max_dates, wm_caps = cap_watermark_advance_at_low_volume(
        df_bronze, sheet_max_dates, watermark_map,
        eligible_low_cap=future_grains,
    )
    if wm_caps:
        low_caps = [c for c in wm_caps if c.get("kind", "low_cap") == "low_cap"]
        high_flags = [c for c in wm_caps if c.get("kind") == "high_flag"]
        for cap in low_caps:
            print(
                f"[WATERMARK] cap ({cap['creds']}, {cap['sheet_name']}, {cap['toko']}) "
                f"{cap['from_date']} -> {cap['capped_at']} "
                f"(low volume {cap['count']} < band lower {cap['lower_bound']:.1f}, "
                f"median {cap['median_count']:.1f}, n={cap['n_dates']} days)"
            )
        for h in high_flags:
            for hd, hc in h["high_dates"]:
                print(
                    f"[WATERMARK] high ({h['creds']}, {h['sheet_name']}, {h['toko']}) "
                    f"{hd} volume {hc} > band upper {h['high_bound']:.1f}, "
                    f"median {h['median_count']:.1f}, n={h['n_dates']} days"
                )
        if low_caps:
            emit(
                "BRONZE", "watermark_cap",
                f"Capped watermark advance for {len(low_caps)} grain(s) on low-volume boundary date",
                level="WARN",
                metrics={"capped": low_caps},
                source=SRC_GSHEET,
                target=SRC_MINIO,
            )
        if high_flags:
            emit(
                "BRONZE", "watermark_spike",
                f"Flagged high daily volume for {len(high_flags)} grain(s) (no watermark change)",
                level="WARN",
                metrics={"high_flags": high_flags},
                source=SRC_GSHEET,
                target=SRC_MINIO,
            )

    if not df_bronze.empty:
        batch_dates = df_bronze["tanggal"].dropna()
        min_date = str(batch_dates.min().date()) if not batch_dates.empty else None
        existing_hashes = fetch_existing_bronze_hashes(creds, table_id="BRONZE_DB.bronze_affiliate", project_id=PROJECT_ID, min_date=min_date)
        if existing_hashes:
            before = len(df_bronze)
            df_bronze = df_bronze[
                ~df_bronze["row_hash_raw"].astype(str).isin(existing_hashes)
            ]
            skipped = before - len(df_bronze)
            if skipped:
                print(
                    f"[IDEMPOTENCY] Skipped {skipped} row(s) already present in bronze"
                )
                emit(
                    "BRONZE", "idempotency_gate",
                    f"Skipped {skipped} row(s) already present in bronze",
                    level="INFO",
                    metrics={"skipped": int(skipped)},
                    source=SRC_GSHEET,
                    target=TGT_BQ_BRONZE,
                )

    print(f"[BRONZE] Rows bronze to load: {len(df_bronze)}")
    emit("BRONZE", "bronze_builder", f"Rows bronze to load: {len(df_bronze)}",
         metrics={"rows_loaded": int(len(df_bronze))},
         source=SRC_GSHEET,
         target=TGT_BQ_BRONZE)

    if df_bronze.empty and sheet_max_dates:
        if dry_run:
            changes = effective_watermark_changes(watermark_records, sheet_max_dates)
            for (sheet_key, sheet_name, toko), max_date in changes.items():
                print(f"[DRY-RUN]   watermark update ({sheet_key}, {sheet_name}, {toko}) -> {max_date}")
            if changes:
                print("[DRY-RUN] Akan update watermark (tanpa upload parquet).")
            else:
                print("[DRY-RUN] Tidak ada perubahan watermark (nilai sudah sama).")
            emit("FINISH", "etl_pipeline", "ETL DONE (DRY-RUN) - watermark only",
                 metrics={"watermark_changes": len(changes)},
                 source=SRC_GSHEET, target=SRC_MINIO)
            finish(
                watermark_records,
                "== ETL Pesanan Affiliasi DONE (DRY-RUN) ==",
                status="dry-run — watermark only",
                dry_run=dry_run,
                run_key=run_key,
                watermark_updates=sheet_max_dates,
                bq_updates=bq_noop(),
            )
            return
        update_sheet_watermarks(
            minio_client, minio_bucket, WATERMARK_PATH, watermark_records,
            sheet_max_dates, sheet_registry=sheet_registry,
        )
        print("[MINIO] Watermark advanced (no new rows to append).")
        emit("FINISH", "etl_pipeline", "ETL DONE - watermark advanced (no new rows)",
             metrics={"watermark_changes": len(sheet_max_dates)},
             source=SRC_GSHEET, target=SRC_MINIO)
        finish(
            watermark_records,
            "== ETL Pesanan Affiliasi DONE ==",
            status="no new rows — watermark advanced",
            dry_run=dry_run,
            run_key=run_key,
            watermark_updates=sheet_max_dates,
            bq_updates=bq_noop(),
        )
        return

    if df_bronze.empty:
        print("[MINIO] No new data to process. Data is up-to-date.")
        emit("FINISH", "etl_pipeline", "ETL DONE - no new data to process",
             metrics={"rows_loaded": 0},
             source=SRC_GSHEET, target=SRC_MINIO)
        finish(
            watermark_records,
            "== ETL Pesanan Affiliasi DONE ==",
            status="data is up to date — no new rows",
            dry_run=dry_run,
            run_key=run_key,
            bq_updates=bq_noop(),
        )
        return

    file_path = f"pesanan/affiliasi/date={today_key}/affiliasi_{run_key}.parquet"
    folder_path = f"pesanan/affiliasi/date={today_key}/"

    if dry_run:
        print(f"[DRY-RUN] Akan upload {len(df_bronze)} baris ke '{file_path}'")
        changes = effective_watermark_changes(watermark_records, sheet_max_dates)
        for (sheet_key, sheet_name, toko), max_date in changes.items():
            print(f"[DRY-RUN]   watermark update ({sheet_key}, {sheet_name}, {toko}) -> {max_date}")
        if not changes:
            print("[DRY-RUN] Tidak ada perubahan watermark (nilai sudah sama).")
        print("[DRY-RUN] Akan: append ke BRONZE_DB.bronze_affiliate + MERGE ke silver_tt_affiliate")
        print("[DRY-RUN] Selesai. TIDAK ada data yang ditulis (dry-run).")
        emit("FINISH", "etl_pipeline",
             f"ETL DONE (DRY-RUN) - would load {len(df_bronze)} rows",
             metrics={"rows_loaded": int(len(df_bronze)),
                      "watermark_changes": len(changes)},
             source=SRC_GSHEET, target=TGT_BQ_BRONZE)
        finish(
            watermark_records,
            "== ETL Pesanan Affiliasi DONE (DRY-RUN) ==",
            status="dry-run — full load (would run)",
            dry_run=dry_run,
            run_key=run_key,
            watermark_updates=changes,
            bq_updates=bq_full_load(len(df_bronze)),
        )
        return

    minio_client.put_object(minio_bucket, folder_path, io.BytesIO(b""), length=0)

    try:
        parquet_bytes = df_bronze.to_parquet(index=False, engine="pyarrow")
        minio_client.put_object(
            minio_bucket,
            file_path,
            io.BytesIO(parquet_bytes),
            length=len(parquet_bytes),
            content_type="application/octet-stream",
        )
    except Exception as e:
        emit(
            "LOAD", "minio_loader",
            f"Parquet upload failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={"rows": int(len(df_bronze)), "target_path": file_path},
            source=SRC_GSHEET,
            target=TGT_MINIO,
        )
        if log_folder:
            err_lines = [
                f"=== BRONZE/PARQUET ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nDataset: pesanan_affiliasi",
                f"  Stage: Parquet upload",
                f"  Target path: {file_path}",
                f"  Rows: {len(df_bronze)}",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"bronze_parquet_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        _failure_ctx.update({
            "stage": "MinIO parquet upload",
            "minio_files": [folder_path, file_path],
            "rollback_hint": (
                "Watermark is NOT yet updated and BigQuery is untouched. "
                "A partial parquet may exist in MinIO; a re-run is safe "
                "and needs no rollback."
            ),
        })
        raise
    print(f"[MINIO] Successfully uploaded Parquet file to: {file_path}")
    emit("LOAD", "minio_loader", f"Parquet uploaded to {file_path}",
         metrics={"rows": int(len(df_bronze)), "target_path": file_path},
         source=SRC_GSHEET, target=TGT_MINIO)

    update_sheet_watermarks(
        minio_client, minio_bucket, WATERMARK_PATH, watermark_records, sheet_max_dates,
        sheet_registry=sheet_registry,
    )

    try:
        load_df(
            df_bronze,
            table_id="BRONZE_DB.bronze_affiliate",
            project_id=PROJECT_ID,
            if_exists="append",
            credentials=creds,
        )
    except Exception as e:
        emit(
            "LOAD", "bigquery_loader",
            f"BigQuery load failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={
                "rows_loaded": int(len(df_bronze)),
                "table": f"{PROJECT_ID}.BRONZE_DB.bronze_affiliate",
                "if_exists": "append",
            },
            source=SRC_MINIO,
            target=TGT_BQ_BRONZE,
        )
        if log_folder:
            err_lines = [
                f"=== BRONZE/PARQUET ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nDataset: pesanan_affiliasi",
                f"  Stage: BigQuery load",
                f"  Target table: BRONZE_DB.bronze_affiliate",
                f"  Rows being loaded: {len(df_bronze)}",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"bronze_parquet_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        _failure_ctx.update({
            "stage": "BigQuery bronze load (append)",
            "minio_files": [file_path, WATERMARK_PATH],
            "rollback_hint": (
                "Watermark NOT updated — a re-run will safely re-select "
                "rows. No manual rollback needed."
            ),
        })
        raise
    print("[BRONZE] Load to BRONZE_DB.bronze_affiliate DONE")
    emit("LOAD", "bigquery_loader", "Bronze load to BRONZE_DB.bronze_affiliate DONE",
         metrics={"rows_loaded": int(len(df_bronze)),
                  "table": f"{PROJECT_ID}.BRONZE_DB.bronze_affiliate"},
         source=SRC_MINIO, target=TGT_BQ_BRONZE)

    print("[SILVER] Running MERGE into SILVER_DB.silver_tt_affiliate ...")
    try:
        merge_to_silver()
    except Exception as e:
        emit(
            "SILVER", "silver_merger",
            f"Silver MERGE failed: {type(e).__name__} {e}",
            level="ERROR",
            metrics={"table": f"{PROJECT_ID}.SILVER_DB.silver_tt_affiliate"},
            source=TGT_BQ_BRONZE,
            target=TGT_BQ_SILVER,
        )
        if log_folder:
            err_lines = [
                f"=== SILVER/GOLD ERROR - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===",
                f"\nStage: Silver MERGE",
                f"  Table: SILVER_DB.silver_tt_affiliate",
                f"  Error: {type(e).__name__} {e}\n",
                f"  Traceback:\n{traceback.format_exc()}",
            ]
            write_section_log(log_folder, f"silver_gold_errors_{run_key}.log", "\n".join(err_lines) + "\n")
        _failure_ctx.update({
            "stage": "BigQuery silver MERGE",
            "minio_files": [file_path, WATERMARK_PATH],
            "rollback_hint": (
                "MinIO archive + bronze append succeeded; only the silver MERGE failed. "
                "The silver upsert is idempotent over bronze, so a plain re-run is safe "
                "without any MinIO rollback."
            ),
        })
        raise
    print("[SILVER] MERGE DONE")
    emit("SILVER", "silver_merger", "Silver MERGE into SILVER_DB.silver_tt_affiliate DONE",
         metrics={"table": f"{PROJECT_ID}.SILVER_DB.silver_tt_affiliate"},
         source=TGT_BQ_BRONZE, target=TGT_BQ_SILVER)

    emit("FINISH", "etl_pipeline", "ETL Pesanan Affiliasi DONE",
         metrics={"rows_loaded": int(len(df_bronze))},
         source=SRC_GSHEET, target=TGT_BQ_SILVER)

    finish(
        watermark_records,
        "== ETL Pesanan Affiliasi DONE ==",
        status="full load finished — Silver MERGE done",
        dry_run=dry_run,
        run_key=run_key,
        watermark_updates=sheet_max_dates,
        bq_updates=bq_full_load(len(df_bronze)),
    )


if __name__ == "__main__":
    run_daily_etl()
