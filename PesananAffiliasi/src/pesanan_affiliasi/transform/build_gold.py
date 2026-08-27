# src/pesanan_affiliasi/transform/build_gold.py
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

def _load_sql(fname: str) -> str:
    root_dir = Path(__file__).resolve().parents[3]  # etl-pesanan-affiliasi/
    sql_path = root_dir / "sql" / fname
    return sql_path.read_text(encoding="utf-8")


def build_fact_affiliate(bq_client: bigquery.Client) -> pd.DataFrame:
    # 1) ambil pesanan dari silver_tt_affiliate
    tt_affiliate = _load_sql("select_silver_tt_affiliate.sql")
    df_affiliate = bq_client.query(tt_affiliate).to_dataframe()

    df_affiliate["tanggal"] = pd.to_datetime(df_affiliate["tanggal"], errors="coerce").dt.date
    
    # kolom silver bernama nama_pengguna_kreator -> creator_username utk merge
    if "nama_pengguna_kreator" in df_affiliate.columns:
        df_affiliate["creator_username"] = (
            df_affiliate["nama_pengguna_kreator"]
            .astype(str)
            .str.strip()
            .str.upper()
        )
    elif "creator_username" in df_affiliate.columns:
        df_affiliate["creator_username"] = (
            df_affiliate["creator_username"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

    # 2) ambil akun affiliate dari bronze_akun_affiliate
    akun_affiliate = _load_sql("select_bronze_akun_affiliate.sql")
    df_akun_affiliate = bq_client.query(akun_affiliate).to_dataframe()

    df_akun_affiliate["creator_username"] = (
        df_akun_affiliate["creator_username"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # 3) aggregasi akun affiliate per creator username
    agg_df_akun = df_akun_affiliate.groupby("creator_username").agg(
        {
            "jenis_affiliate": "first",  
            "status_affiliate": "first",
        }
    ).reset_index()

    # 4) merge
    merged_df = agg_df_akun[
        ["creator_username","jenis_affiliate", "status_affiliate"]
    ].merge(
        df_affiliate[
            [
                "tanggal",
                "toko",
                "platform",
                "id_pesanan",
                "id_produk",
                "produk",
                "sku",
                "id_sku",
                "penjual_sku",
                "mata_uang",
                "harga",
                "payment_amount",
                "kuantitas",
                "metode_pembayaran",
                "status_pesanan",
                "creator_username",
                "jenis_konten",
                "id_konten",
                "commission_model",
                "persen_komisi_std",
                "est_acuan_komisi",
                "perkiraan_bayar_komisi_std",
                "acuan_komisi_aktual",
                "pembayaran_komisi_aktual",
                "persen_komisi_iklan_belanja",
                "perkiraan_bayar_komisi_iklan_belanja",
                "bayar_komisi_iklan_belanja_aktual",
                "perkiraan_bonus_ditanggungkan",
                "bonus_sebenarnya_ditanggungkan",
                "pengembalian_barang",
                "pengembalian_dana",
                "waktu_dibuat",
                "waktu_pembayaran",
                "waktu_pesanan_siap_dikirim",
                "order_delivery_time",
                "waktu_pesanan_selesai",
                "waktu_komisi_dibayar",
                "snapshot_ts",
                "snapshot_date",
                "run_id",
                "row_hash_raw",
                "row_hash_clean",
            ]
        ],
        on="creator_username",
        how="right",
    )

    return merged_df