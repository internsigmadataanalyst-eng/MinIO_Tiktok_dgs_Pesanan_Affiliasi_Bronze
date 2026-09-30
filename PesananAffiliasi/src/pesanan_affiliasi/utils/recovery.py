# src/pesanan_affiliasi/utils/recovery.py
"""Error-recovery path-A logic: select rows from resolved / changed errors."""

import pandas as pd

from src.pesanan_affiliasi.utils.transform_utils import to_snake_case


def _id_pesanan_present(df: pd.DataFrame) -> pd.Series:
    """Boolean mask of rows build_bronze_affiliate will actually keep.

    build_bronze_affiliate hard-drops rows whose `id_pesanan` is blank
    (clean_bronze.py, after the snake_case pass). Applying the same test here
    keeps the recovery counters honest: a row that is about to be dropped is not
    reported as re-admitted. This does not change what reaches bronze.
    """
    # df_valid still carries the raw GSheet headers, so match on the snake_case
    # form rather than hard-coding one spelling.
    snake = {to_snake_case(c): c for c in df.columns}
    col = snake.get("id_pesanan")
    if col is None:
        return pd.Series(True, index=df.index)
    return df[col].astype(str).str.strip() != ""


def select_recovered(
    df_valid: pd.DataFrame,
    resolved: list,
    report: dict,
    readmit: list | None = None,
) -> pd.DataFrame:
    """Select rows from *df_valid* that belong to a recovered error group.

    Grain is (sheet_name, creds, toko, error_date) -- toko verbatim. Two kinds of
    group are passed in, and both are treated identically: select EVERY valid row
    carrying the group's key.

      - `resolved`: the group is no longer detected as bad this run (defect gone,
        or a date_future / legacy future-date entry remediated).
      - `readmit` : the group is still detected, but its signature
        (n_rows, error_reasons) changed -- some rows were fixed, or the defect
        changed shape.

    Re-admitting a whole group is deliberately NOT count-matched. A strict
    `count == n_rows` test could never fire for a partially fixed date: fixing
    some rows lowers the count while the group stays open, so the entry
    deadlocks forever and the fixed rows are dropped by the watermark filter.
    Here the whole group is re-admitted and the downstream layers sort it out:
    `row_hash_raw` includes `tanggal`, the id columns, the three `waktu_*`
    columns and `status_pesanan`, so a value-corrected row gets a fresh hash and
    survives the bronze idempotency gate as a new revision, while an unchanged
    row collides with its bronze twin and is dropped. The silver MERGE then
    de-duplicates bronze by business key (ROW_NUMBER ... ORDER BY
    snapshot_ts DESC, run_id DESC) and only UPDATEs when row_hash_clean
    differs -- so the correction lands without double-counting.

    KNOWN GAP (numeric_mixed): `row_hash_raw` deliberately excludes every
    numeric column, so a defect that is fixed ONLY in a numeric value produces
    a row with an IDENTICAL hash to the bad version already in bronze. The
    idempotency gate in run_daily_etl then drops the corrected row and the fix
    never reaches bronze, even though it was correctly re-admitted here. The
    counters below are therefore honest about selection but not about landing:
    `recovery_partial` counts a group whose signature changed, which for
    numeric_mixed may still land zero rows. Closing this needs a change to
    cols_for_hash in clean_bronze.py, which would re-hash every historical row
    and is out of scope for the quarantine work.

    Callers must pass these rows through build_bronze_affiliate with an EMPTY
    watermark map so the watermark date cannot gate them; the returned frame
    bypasses the regular incremental path.

    Counters added to ``report``:
      recovery_resolved        : fully resolved groups considered
      recovery_partial         : still-broken groups whose signature changed
      recovery_readmit_rows    : rows selected for Path A
      recovery_absent          : groups with no matching valid rows
    """
    df = df_valid.copy()

    candidates = list(resolved or []) + list(readmit or [])

    if df.empty or not candidates:
        report.setdefault("recovery_resolved", 0)
        report.setdefault("recovery_partial", 0)
        report.setdefault("recovery_readmit_rows", 0)
        report.setdefault("recovery_absent", 0)
        return df.iloc[0:0]

    if "Toko" in df.columns:
        toko_series = df["Toko"].astype(str)
    elif "toko" in df.columns:
        toko_series = df["toko"].astype(str)
    else:
        toko_series = pd.Series("", index=df.index, dtype=str)

    try:
        tanggal_str = df["Tanggal"].dt.date.astype(str)
    except Exception:
        tanggal_str = pd.to_datetime(df["Tanggal"]).dt.date.astype(str)

    key_series = (
        df["sheet_name"].astype(str)
        + "|" + df["creds"].astype(str)
        + "|" + toko_series
        + "|" + tanggal_str
    )

    match = pd.Series(False, index=df.index)
    absent = 0

    for r in candidates:
        key = f'{r["sheet_name"]}|{r["creds"]}|{r.get("toko") or ""}|{r["error_date"]}'
        grp = df.index[key_series == key]
        if len(grp) == 0:
            absent += 1
        else:
            match.loc[grp] = True

    # Drop rows build_bronze_affiliate would discard anyway, so the reported
    # count matches what can actually be appended.
    match = match & _id_pesanan_present(df)

    report["recovery_resolved"] = len(resolved or [])
    report["recovery_partial"] = len(readmit or [])
    report["recovery_readmit_rows"] = int(match.sum())
    report["recovery_absent"] = absent

    return df[match]
