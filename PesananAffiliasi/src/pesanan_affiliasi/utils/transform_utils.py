# src/pesanan_affiliasi/utils/transform_utils.py
import re
import warnings
from datetime import datetime
import numpy as np
import pandas as pd
from typing import Any, List

EXCEL_EPOCH = pd.Timestamp("1899-12-30")

NUMERIC_COLS = [
    "Harga",
    "Payment Amount",
    "Kuantitas",
    "Est. Acuan Komisi",
    "Perkiraan pembayaran komisi standar",
    "Acuan Komisi Aktual",
    "Pembayaran Komisi Aktual",
    "Perkiraan pembayaran komisi Iklan Belanja",
    "Pembayaran komisi Iklan Belanja aktual",
    "Perkiraan bonus yang ditanggung bersama untuk kreator",
    "Bonus sebenarnya yang ditanggung bersama untuk kreator",
    "Pengembalian barang",
    "Pengembalian dana",
]

# Kolom persentase komisi TIDAK divalidasi/dibersihkan di sini: format
# campurannya ("5" = 5%, "0.05" = 5%, "5%" = 5%) dinormalisasi ke pecahan
# 0..1 oleh clean_bronze.mixed_percentage setelah tahap validasi.
# List dibiarkan kosong agar detect_numeric_corruption mengabaikannya.
PERCENT_COLS = [
]

# Kolom ID (pesanan/produk/sku/konten). Nilai valid harus digit murni (0-9);
# scientific notation ("7,49479E+18"), desimal, atau teks yang tergeser menandakan
# baris korup (perubahan format / column shift). Sel KOSONG dianggap absen dan
# TIDAK ditandai (id_konten bisa legitimate kosong utk sebagian jenis konten).
# Var iant snake_case ikut dicek agar tetap bekerja di frame yang sudah di-clean.
ID_COLS = [
    "ID Pesanan", "id_pesanan",
    "ID Produk", "id_produk",
    "ID SKU", "ID Sku", "id_sku",
    "ID Konten", "id_konten",
]

# Kolom teks (non-numeric) yang TIDAK boleh berisi ID berdigit-panjang.
# Jika isinya menyerupai ID (6-22 digit murni / scientific notation), ada nilai
# ID yang tergeser dari kolom ID lain -> baris korup (column shift).
TEXT_COLS = [
    "Platform", "platform",
    "Metode Pembayaran", "metode_pembayaran",
    "Nama Pengguna Kreator", "nama_pengguna_kreator",
    "Jenis Konten", "jenis_konten",
    "Commission Model", "commission_model",
]

# A valid numeric cell may only contain digits / '.' / ',' / 'Rp' / whitespace /
# '#' / '-' (both are censored markers: '######' or '-' mean the value is
# hidden / could not be displayed, e.g. a too-narrow column, not a real error).
# Anything else (e.g. '%', letters) means the cell does not belong to a
# count/currency column (typically a shifted value from another column).
_NUMERIC_FORBIDDEN = re.compile(r"[^0-9.,Rp\s#-]")

# A valid rate/percentage cell may only contain digits / '.' / ',' / '%' /
# whitespace / '#' / '-'. Plain '0' or '0,00%' is valid; pure text (e.g.
# 'IDR') is not, while '######' / '-' are treated as censored data, not errors.
_PERCENT_FORBIDDEN = re.compile(r"[^0-9.,%\s#-]")

# Sel ID valid: digit 0-9 saja. Karakter lain ('E','+',',','.', huruf) = korup.
_ID_FORBIDDEN = re.compile(r"[^0-9]")

# Nilai menyerupai ID panjang (6-22 digit) atau scientific notation -> indikasi
# bahwa sebuah ID tergeser masuk ke kolom teks.
_BIG_INT_RE = re.compile(r"^\d{6,22}$")
_SCIENTIFIC_RE = re.compile(r"^\d+[.,]?\d*[Ee][+-]?\d+$")

def to_snake_case(column_name: str) -> str:
    return (
        column_name.lower()
        .strip()
        .replace(" ", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "")
        .replace("/", "")
        .replace("-", "")
    )


def _coerce_numeric_series(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Cleans a raw string series into numerics using the exact same steps as
    clean_numeric_columns. Returns (numeric_values, cleaned_string_series)."""
    s = series.astype(str).str.strip()
    s = s.replace("-", np.nan)
    s = s.str.replace(r"[^\d,\.]", "", regex=True)
    s = s.str.replace(".", "", regex=False)
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce"), s


def clean_numeric_columns(df: pd.DataFrame, cols, fillna_value=0) -> pd.DataFrame:
    df = df.copy()

    for col in cols:
        if col not in df.columns:
            print(f"Kolom '{col}' tidak ditemukan di DataFrame. Lewati Nggih.")
            continue

        df[col] = df[col].astype(str)
        df[col] = df[col].replace("-", np.nan)
        df[col] = df[col].str.replace(r"[^\d,\.]", "", regex=True)
        df[col] = df[col].str.replace(".", "", regex=False)
        df[col] = df[col].str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(fillna_value)

        if (df[col] % 1 == 0).all():
            df[col] = df[col].astype(int)

    return df


def detect_blank_rows(
    df: pd.DataFrame, ignore_cols: List[str] = None
) -> pd.Series:
    """Flags rows where every non-tag column is blank.

    Tag/metadata columns (creds, sheet_name, error_reason, ...) are excluded
    from the blank check so fully-empty source rows are detected even though
    their tags are populated.
    """
    if ignore_cols is None:
        ignore_cols = ["creds", "sheet_name", "error_reason"]

    data_cols = [c for c in df.columns if c not in ignore_cols]
    if not data_cols:
        return pd.Series(False, index=df.index)

    def _is_blank(v: Any) -> bool:
        if pd.isna(v):
            return True
        return str(v).strip().lower() in ("", "-", "nan", "none", "nat")

    sub = df[data_cols]
    blank = pd.DataFrame(
        {c: sub[c].map(_is_blank) for c in sub.columns}, index=sub.index
    )
    return blank.all(axis=1)


def _is_non_empty(raw: pd.Series) -> pd.Series:
    """Mask of cells holding real content (excludes blank / dash / nan variants)."""
    return raw.notna() & ~raw.str.lower().isin(["", "nan", "none", "-", "n/a"])


def detect_numeric_corruption(
    df: pd.DataFrame,
    numeric_cols,
    percent_cols=None,
    date_col: str = "Tanggal",
) -> dict:
    """Row-level mixed-column detection.

    - numeric columns: a cell is corrupt when it holds content and either
      (a) contains any character other than digits / '.' / ',' / 'Rp' /
      whitespace (e.g. '%', letters, '######' -- a shifted value), or
      (b) still fails numeric coercion after the lossy clean (e.g. pure
      punctuation that strips to nothing).
    - percent columns (optional): a cell is corrupt when it holds content and
      contains any character other than digits / '.' / ',' / '%' / whitespace
      (e.g. 'IDR', '######', text). Plain '0', '125' and '0,00%' stay valid.

    Returns a report with the affected rows mask and date context.
    """
    df = df.copy()
    affected_mask = pd.Series(False, index=df.index)
    corrupted_cols = []

    for col in numeric_cols:
        if col not in df.columns:
            continue

        raw = df[col].astype(str).str.strip()
        values, _ = _coerce_numeric_series(raw)

        is_not_empty = _is_non_empty(raw)
        is_forbidden = raw.str.contains(_NUMERIC_FORBIDDEN, na=False)
        is_corrupted = is_not_empty & (is_forbidden | values.isna())

        if is_corrupted.any():
            corrupted_cols.append(col)
            affected_mask = affected_mask | is_corrupted

    for col in (percent_cols or []):
        if col not in df.columns:
            continue

        raw = df[col].astype(str).str.strip()

        is_not_empty = _is_non_empty(raw)
        is_forbidden = raw.str.contains(_PERCENT_FORBIDDEN, na=False)
        is_corrupted = is_not_empty & is_forbidden

        if is_corrupted.any():
            corrupted_cols.append(col)
            affected_mask = affected_mask | is_corrupted

    report = {
        "has_changes": bool(affected_mask.any()),
        "affected_columns": corrupted_cols,
        "affected_mask": affected_mask,
        "n_bad_rows": int(affected_mask.sum()),
        "affected_dates": [],
        "first_affected_date": None,
        "last_affected_date": None,
    }

    if affected_mask.any() and date_col in df.columns:
        dates = (
            parse_mixed_dates(df.loc[affected_mask, date_col], return_date=False)
            .dropna()
            .sort_values()
            .unique()
            .tolist()
        )
        report["affected_dates"] = dates
        report["first_affected_date"] = dates[0] if dates else None
        report["last_affected_date"] = dates[-1] if dates else None

    return report


def detect_id_and_shift_corruption(
    df: pd.DataFrame, id_cols: list = None, text_cols: list = None
) -> dict:
    """Row-level detection of column-shift / format-change corruption.

    - ID columns (id_cols): a non-empty cell containing anything but digits is
      corrupt (scientific notation '7,49479E+18', decimals, letters, or a
      shifted value). Empty cells are allowed (id_konten may legitimately be
      absent), so only INVALID CONTENT is flagged.
    - TEXT columns (text_cols): a cell that looks like a long integer or
      scientific number means an ID slid into a text column (while its own ID
      column went blank).

    Returns a report with the combined affected mask, the per-rule masks (so
    callers can attach distinct error reasons), and the affected column names.
    """
    df = df.copy()
    affected_mask = pd.Series(False, index=df.index)
    id_mask = pd.Series(False, index=df.index)
    shift_mask = pd.Series(False, index=df.index)
    corrupted_cols = []

    for col in (id_cols or ID_COLS):
        if col not in df.columns:
            continue
        raw = df[col].astype(str).str.strip()
        is_not_empty = _is_non_empty(raw)
        is_forbidden = raw.str.contains(_ID_FORBIDDEN, na=False)
        is_corrupted = is_not_empty & is_forbidden
        if is_corrupted.any():
            corrupted_cols.append(col)
            id_mask = id_mask | is_corrupted

    for col in (text_cols or TEXT_COLS):
        if col not in df.columns:
            continue
        raw = df[col].astype(str).str.strip()
        is_not_empty = _is_non_empty(raw)
        id_like = raw.str.match(_BIG_INT_RE, na=False) | raw.str.match(
            _SCIENTIFIC_RE, na=False
        )
        is_corrupted = is_not_empty & id_like
        if is_corrupted.any():
            corrupted_cols.append(col)
            shift_mask = shift_mask | is_corrupted

    affected_mask = id_mask | shift_mask
    return {
        "affected_mask": affected_mask,
        "id_non_digit_mask": id_mask,
        "id_shift_mask": shift_mask,
        "affected_columns": list(dict.fromkeys(corrupted_cols)),
    }


def validate_and_normalize_raw(
    df: pd.DataFrame, numeric_cols, date_col: str = "Tanggal", percent_cols: list = None
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """STEP 2: row-level validation & normalization.

    - Detects mixed/corrupted cells in numeric columns (main filter) and, when
      given, in percentage/rate columns (percent_cols).
    - Parses mixed dates; rows with NaT are captured as date-error rows.
    - Blank `Toko` rows are quarantined (toko_blank) — watermark grain is (creds,sheet_name,toko).
    - Splits into (df_valid, df_error, report). df_error keeps the ORIGINAL raw
      values plus an error_reason column; df_valid is cleaned for downstream.
    """
    df = df.copy()

    blank_mask = detect_blank_rows(df)

    corruption = detect_numeric_corruption(
        df, numeric_cols, percent_cols=percent_cols, date_col=date_col
    )
    id_report = detect_id_and_shift_corruption(df)
    df_clean = clean_numeric_columns(df, numeric_cols, fillna_value=0)

    parsed = parse_mixed_dates(df_clean[date_col], return_date=False)
    df_clean[date_col] = parsed
    date_error = parsed.isna()

    # Future-date gate (soft quarantine): a validly-parsed date strictly after
    # today is a wrong input. It is routed to df_error so it never reaches the
    # watermark filter (thus cannot advance the watermark / poison ingestion).
    today_ts = pd.Timestamp(datetime.now().date())
    date_future = parsed.notna() & (parsed > today_ts)

    toko_col = "Toko" if "Toko" in df.columns else ("toko" if "toko" in df.columns else None)
    if toko_col is not None:
        raw_toko = df[toko_col].astype(str).str.strip()
        toko_blank = raw_toko.str.lower().isin(["", "-", "nan", "none", "nat"])
        toko_blank = toko_blank | df[toko_col].isna()
    else:
        toko_blank = pd.Series(False, index=df.index)

    error_mask = (corruption["affected_mask"] | id_report["affected_mask"] | date_error | toko_blank | date_future) & ~blank_mask

    df_error = df[error_mask].copy()
    reasons = []
    for idx in df_error.index:
        reason_parts = []
        if corruption["affected_mask"].loc[idx]:
            reason_parts.append("numeric_mixed")
        if id_report["id_non_digit_mask"].loc[idx]:
            reason_parts.append("id_non_digit")
        if id_report["id_shift_mask"].loc[idx]:
            reason_parts.append("id_shifted")
        if date_error.loc[idx]:
            reason_parts.append("date_unparsable")
        if date_future.loc[idx]:
            reason_parts.append("date_future")
        if toko_blank.loc[idx]:
            reason_parts.append("toko_blank")
        reasons.append("|".join(reason_parts))
    df_error["error_reason"] = reasons

    df_valid = df_clean[~(error_mask | blank_mask)].copy()

    affected_columns = list(
        dict.fromkeys(corruption["affected_columns"] + id_report["affected_columns"])
    )

    report = {
        "has_changes": bool(error_mask.any()),
        "affected_columns": affected_columns,
        "first_affected_date": corruption["first_affected_date"],
        "last_affected_date": corruption["last_affected_date"],
        "affected_dates": corruption["affected_dates"],
        "n_bad_rows": int(error_mask.sum()),
        "n_date_errors": int(date_error.sum()),
        "n_date_future": int(date_future.sum()),
        "n_blank_rows": int(blank_mask.sum()),
        "n_toko_blank": int(toko_blank.sum()),
        "n_id_errors": int(id_report["affected_mask"].sum()),
    }

    return df_valid, df_error, report


def parse_mixed_dates(series: pd.Series, return_date=True) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": np.nan, "-": np.nan, "nan": np.nan, "None": np.nan})

    s_norm = s.str.replace(r"[-\.]", "/", regex=True)

    mask_ymd = s_norm.str.match(r"^\s*\d{4}/\d{1,2}/\d{1,2}\s*$", na=False)
    ymd = pd.to_datetime(s_norm.where(mask_ymd), format="%Y/%m/%d", errors="coerce")

    mask_dmy4 = s_norm.str.match(r"^\s*\d{1,2}/\d{1,2}/\d{4}\s*$", na=False)
    dmy4 = pd.to_datetime(s_norm.where(mask_dmy4), format="%d/%m/%Y", errors="coerce")

    mask_dmy2 = s_norm.str.match(r"^\s*\d{1,2}/\d{1,2}/\d{2}\s*$", na=False)
    dmy2 = pd.to_datetime(s_norm.where(mask_dmy2), format="%d/%m/%y", errors="coerce")

    mask_serial = s.str.match(r"^\d{3,6}$", na=False)
    serial_vals = pd.to_numeric(s.where(mask_serial), errors="coerce")
    serial = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    serial.loc[mask_serial] = EXCEL_EPOCH + pd.to_timedelta(
        serial_vals.loc[mask_serial], unit="D"
    )

    remaining_mask = (
        ymd.isna() & dmy4.isna() & dmy2.isna() & serial.isna() & ~s.isna()
    )
    iso_generic = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    if remaining_mask.any():
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            iso_generic.loc[remaining_mask] = pd.to_datetime(
                s.loc[remaining_mask], errors="coerce", format="mixed"
            )

    parsed = (
        ymd.combine_first(dmy4)
        .combine_first(dmy2)
        .combine_first(serial)
        .combine_first(iso_generic)
    )

    if return_date:
        return parsed.dt.date

    return parsed


WM_LOWER_PCT = 0.4
WM_UPPER_PCT = 3.0
WM_FLOOR_LOW = 2.0
WM_FLOOR_HIGH = 5.0
WM_MIN_WINDOW = 5


def _tail_volume_stats(
    tail: pd.Series,
    lower_pct: float,
    upper_pct: float,
    floor_low: float,
    floor_high: float,
    min_window: int,
) -> tuple[float | None, float | None]:
    """Robust low/high band boundaries for a grain's tail of daily counts.

    tail: Series of {date: count} strictly after the previous watermark and
    <= today. The reference median is computed on the tail only (the boundary
    re-pull day never inflates it).

    low_bound = max(median * lower_pct, floor_low)  when enough dates, else
    just floor_low -> small stores (median 1-3) still flag a 1-row day.
    high_bound = max(median * upper_pct, floor_high)  (flag-only band).

    Returns (low_bound, high_bound). low_bound is None when the tail has no
    usable median.
    """
    if tail is None or len(tail) == 0:
        return None, None
    median_count = float(tail.median())
    if not np.isfinite(median_count) or median_count <= 0:
        return None, None

    enough = len(tail) >= min_window
    low_bound = max(median_count * lower_pct, floor_low) if enough else floor_low
    high_bound = max(median_count * upper_pct, floor_high)
    return low_bound, high_bound


def cap_watermark_advance_at_low_volume(
    df: pd.DataFrame,
    sheet_max_dates: dict,
    prev_watermarks: dict,
    creds_col: str = "creds",
    sheet_name_col: str = "sheet_name",
    toko_col: str = "toko",
    date_col: str = "tanggal",
    lower_pct: float = WM_LOWER_PCT,
    upper_pct: float = WM_UPPER_PCT,
    floor_low: float = WM_FLOOR_LOW,
    floor_high: float = WM_FLOOR_HIGH,
    min_window: int = WM_MIN_WINDOW,
    eligible_low_cap: set | None = None,
) -> tuple[dict, list]:
    """Adaptive per-toko watermark volume guard.

    A wrongly/prematurely typed row (e.g. a future date that later becomes
    "today") carries a tiny row count vs its neighbors. Advancing the watermark
    to such a date pins the sheet max there and, worse, silently drops the
    genuine rows for that date that arrive late. This rule never advances the
    watermark past the first below-band volume date after the previous
    watermark, so the sheet stays "behind", the gate keeps passing, and late
    genuine rows are re-pulled (hash idempotency dedup absorbs the early rows).

    Every toko's volume signature is store-specific (some process 1-3 rows/day,
    others 50-1500+/day), so the band is per-grain:
      - reference median is computed on THIS run's tail only (dates strictly
        after the previous watermark, <= today) -> adapts to stores in decline
        or growth, and the boundary re-pull day never skews it;
      - low band = max(median * lower_pct, floor_low): the absolute floor keeps
        small stores detectable (a 1-row day is always below-band even when
        median * lower_pct < 1);
      - with fewer than min_window tail dates the ratio band is not applied,
        only the absolute floor (no over-capping on thin evidence);
      - high band = max(median * upper_pct, floor_high) is a FLAG-ONLY detection
        surfaced in reports (kind='high_flag'); a busy day / bulk backfill of
        real rows must never stall the re-pull, so high dates do NOT cap.

    eligible_low_cap optionally restricts LOW-side capping to a given set of
    (sheet_name, toko) pairs (None = cap every grain as before). Grains outside
    the set keep their full sheet max (processed fully); their HIGH-side spike
    is still detected and emitted as kind='high_flag'. Pass an empty set to
    globally disable low-volume capping while keeping spike detection.

    Cost of a genuinely quiet day: the watermark lags one date behind that day
    and re-pulls + dedups that date until its volume normalizes.

    Returns (capped_sheet_max_dates, reports) where reports holds one dict per
    affected grain: kind='low_cap' (watermark held back) or kind='high_flag'
    (spike detected, watermark not affected).
    """
    if df is None or df.empty or not sheet_max_dates:
        return sheet_max_dates, []

    capped = dict(sheet_max_dates)
    reports = []

    if not all(c in df.columns for c in (creds_col, sheet_name_col, toko_col, date_col)):
        return sheet_max_dates, []

    parsed = pd.to_datetime(df[date_col])
    today_ts = pd.Timestamp(datetime.now().date())

    def _grain_key(series_row_vals):
        return tuple(str(series_row_vals[c]) for c in (creds_col, sheet_name_col, toko_col))

    df_key = (
        df[creds_col].astype(str)
        + "|" + df[sheet_name_col].astype(str)
        + "|" + df[toko_col].astype(str)
    )

    for key, max_date in sheet_max_dates.items():
        k = tuple(str(x or "") for x in key)

        grain_mask = df_key == k[0] + "|" + k[1] + "|" + k[2]
        if not grain_mask.any():
            continue

        dates = parsed[grain_mask]
        counts = dates.value_counts()

        prev_str = str((prev_watermarks or {}).get(k, "") or "").strip()[:10]
        try:
            prev_ts = pd.Timestamp(prev_str) if prev_str else pd.NaT
        except Exception:
            prev_ts = pd.NaT

        # Only the dates strictly AFTER the previous watermark represent the
        # advance; the boundary date (== watermark) is a legitimate re-pull and
        # is excluded from the volume reference.
        tail = counts[counts.index > prev_ts] if pd.notna(prev_ts) else counts
        tail = tail[tail.index <= today_ts]
        if tail.empty:
            continue

        low_bound, high_bound = _tail_volume_stats(
            tail, lower_pct, upper_pct, floor_low, floor_high, min_window
        )
        if low_bound is None:
            continue

        n_dates = int(len(tail))
        median_count = float(tail.median())

        # Restrict LOW-side capping to an explicit (sheet_name, toko) whitelist
        # (default: every grain). Non-eligible grains are processed fully; only
        # their HIGH-side spike stays flagged for observability.
        eligible = (
            eligible_low_cap is None
            or (k[1], k[2]) in (eligible_low_cap or set())
        )

        # HIGH side: flag-only detection (never caps).
        above = tail[tail > high_bound]
        high_dates = [
            [t.date().isoformat(), int(c)] for t, c in above.items()
        ]
        if above.empty:
            high_bound_eff = None
        else:
            high_bound_eff = high_bound

        if not eligible:
            if not above.empty:
                reports.append({
                    "creds": k[0],
                    "sheet_name": k[1],
                    "toko": k[2],
                    "from_date": pd.Timestamp(max_date).date().isoformat(),
                    "median_count": median_count,
                    "lower_bound": low_bound,
                    "n_dates": n_dates,
                    "high_bound": high_bound_eff,
                    "high_dates": high_dates,
                    "kind": "high_flag",
                })
            continue

        # LOW side: cap watermark advance at the first below-band date so late
        # genuine rows for that date aren't dropped by the watermark.
        below = tail[tail < low_bound]
        if below.empty and above.empty:
            continue

        report = {
            "creds": k[0],
            "sheet_name": k[1],
            "toko": k[2],
            "from_date": pd.Timestamp(max_date).date().isoformat(),
            "median_count": median_count,
            "lower_bound": low_bound,
            "n_dates": n_dates,
            "high_bound": high_bound_eff,
            "high_dates": high_dates,
        }

        if not below.empty:
            first_low_date = below.index.min()
            ok_up_to = tail[tail.index < first_low_date]
            new_max = ok_up_to.index.max() if not ok_up_to.empty else None

            candidate = pd.Timestamp(max_date)
            if new_max is not None:
                capped_date = min(pd.Timestamp(new_max), candidate)
            elif pd.notna(prev_ts):
                capped_date = min(prev_ts, candidate)
            else:
                capped_date = min(today_ts, candidate)
            if pd.isna(capped_date):
                continue

            capped[key] = capped_date.date().isoformat()
            report.update({
                "kind": "low_cap",
                "capped_at": capped_date.date().isoformat(),
                "first_low_date": first_low_date.date().isoformat(),
                "count": int(below.min()),
            })
        else:
            report["kind"] = "high_flag"

        reports.append(report)

    return capped, reports