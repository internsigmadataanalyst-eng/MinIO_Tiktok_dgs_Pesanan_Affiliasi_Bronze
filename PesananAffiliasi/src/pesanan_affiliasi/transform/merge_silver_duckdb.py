import re
from pathlib import Path
import duckdb
import pandas as pd


def _transpile_bq_to_duckdb(sql_content: str) -> str:
    """Helper untuk merubah sintaks BigQuery SQL agar kompatibel dengan DuckDB di Memory."""
    # 1. Hapus prefix 'database-sigma.' & tanda backtick (`)
    sql_executable = (
        sql_content.replace("`database-sigma.", "")
        .replace("database-sigma.", "")
        .replace("`", "")
    )

    # 2. Sisipkan kata 'INTO' pada klausa MERGE
    sql_executable = re.sub(
        r"\bMERGE\s+(?!INTO\b)", "MERGE INTO ", sql_executable, flags=re.IGNORECASE
    )

    # 3. Ubah EXCEPT(...) BigQuery menjadi EXCLUDE(...) DuckDB
    sql_executable = re.sub(
        r"\bEXCEPT\s*\(", "EXCLUDE (", sql_executable, flags=re.IGNORECASE
    )

    # 4. Ubah BigQuery raw string r'...' menjadi standard string '...' DuckDB
    sql_executable = re.sub(r"\br(['\"][^'\"]*['\"])", r"\1", sql_executable)

    # 5. Ubah SAFE.PARSE_TIME menjadi STRPTIME
    sql_executable = re.sub(
        r"SAFE\.PARSE_TIME\(\s*'%H:%M'\s*,\s*([^)]+)\)",
        r"TRY_CAST(STRPTIME(\1, '%H:%M') AS TIME)",
        sql_executable,
        flags=re.IGNORECASE,
    )

    # 6. Ubah konstruktor TIME(...) menjadi MAKE_TIME(...)
    sql_executable = re.sub(
        r"\bTIME\s*\(", "MAKE_TIME(", sql_executable, flags=re.IGNORECASE
    )

    # 7. Ubah fungsi & tipe data khas BigQuery
    sql_executable = re.sub(
        r"\bSAFE_CAST\b", "TRY_CAST", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bINT64\b", "BIGINT", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bFLOAT64\b", "DOUBLE", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"\bNUMERIC\b", "DECIMAL", sql_executable, flags=re.IGNORECASE
    )
    sql_executable = re.sub(
        r"FORMAT_DATE\(\s*'%F'\s*,\s*([^)]+)\)",
        r"STRFTIME(\1, '%Y-%m-%d')",
        sql_executable,
        flags=re.IGNORECASE,
    )

    return sql_executable


def test_merge_to_silver_duckdb(df_bronze: pd.DataFrame):
    """Menjalankan simulasi MERGE Bronze -> Silver di DuckDB In-Memory."""
    # 1. Init Connection
    con = duckdb.connect(":memory:")
    con.sql("INSTALL bigquery FROM community; LOAD bigquery;")

    # 2. Setup Bronze (pakai schema 'Testing' agar sama dengan SQL produksi)
    con.sql("CREATE SCHEMA IF NOT EXISTS Testing;")
    con.sql("CREATE TABLE Testing.bronze_affiliate AS SELECT * FROM df_bronze")
    print("[BRONZE] Load to Testing.bronze_affiliate DONE")
    print("Show Sampel Data bronze_affiliate:")
    con.sql("SELECT * FROM Testing.bronze_affiliate LIMIT 3").show()

    # 3. Setup Silver Schema & Macro
    con.sql("""
        CREATE TABLE IF NOT EXISTS Testing.silver_tt_affiliate (
            tanggal DATE,
            toko VARCHAR,
            platform VARCHAR,
            id_pesanan VARCHAR,
            id_produk VARCHAR,
            produk VARCHAR,
            sku VARCHAR,
            id_sku VARCHAR,
            penjual_sku VARCHAR,
            mata_uang VARCHAR,
            harga DECIMAL,
            payment_amount DECIMAL,
            kuantitas BIGINT,
            metode_pembayaran VARCHAR,
            status_pesanan VARCHAR,
            nama_pengguna_kreator VARCHAR,
            jenis_konten VARCHAR,
            id_konten VARCHAR,
            commission_model VARCHAR,
            persen_komisi_std DOUBLE,
            est_acuan_komisi DECIMAL,
            perkiraan_bayar_komisi_std DECIMAL,
            acuan_komisi_aktual DECIMAL,
            pembayaran_komisi_aktual DECIMAL,
            persen_komisi_iklan_belanja DOUBLE,
            perkiraan_bayar_komisi_iklan_belanja DECIMAL,
            bayar_komisi_iklan_belanja_aktual DECIMAL,
            perkiraan_bonus_ditanggungkan DECIMAL,
            bonus_sebenarnya_ditanggungkan DECIMAL,
            pengembalian_barang BIGINT,
            pengembalian_dana BIGINT,
            waktu_dibuat VARCHAR,
            waktu_pembayaran VARCHAR,
            waktu_pesanan_siap_dikirim VARCHAR,
            order_delivery_time VARCHAR,
            waktu_pesanan_selesai VARCHAR,
            waktu_komisi_dibayar VARCHAR,
            snapshot_ts VARCHAR,
            snapshot_date DATE,
            run_id VARCHAR,
            row_hash_raw VARCHAR,
            row_hash_clean VARCHAR
        );
    """)
    con.sql("""
        CREATE MACRO DATETIME(d, t) AS TRY_CAST(d || ' ' || t AS TIMESTAMP);
    """)

    # 4. Read & Transpile SQL
    print("[SILVER] Running MERGE into Testing.silver_tt_affiliate ...")
    root_dir = Path(__file__).resolve().parents[3]  # Path ke root etl-data-produk/
    sql_path = root_dir / "sql" / "silver_merge_tt_affiliate.sql"

    sql_content = sql_path.read_text(encoding="utf-8")
    sql_executable = _transpile_bq_to_duckdb(sql_content)

    # 5. Execute MERGE Query
    try:
        con.sql(sql_executable)
        print("✅ MERGE SQL Execution Success!")
    except Exception as e:
        print(f"❌ Error saat eksekusi SQL: {e}")

    print("[SILVER] MERGE DONE")
    print("Show Sampel Data silver_tt_affiliate:")
    con.sql("SELECT * FROM Testing.silver_tt_affiliate LIMIT 3").show()
    con.close()