MERGE INTO database-sigma.SILVER_DB.silver_tt_affiliate T
USING (
  WITH latest_raw AS (
    SELECT * EXCLUDE(rn) FROM (
      SELECT b.*,
             ROW_NUMBER() OVER (
               PARTITION BY UPPER(TRIM(b.toko)),
                            DATE(b.tanggal),
                            UPPER(TRIM(COALESCE(b.id_pesanan,''))),
                            UPPER(TRIM(COALESCE(b.id_produk,''))),
                            UPPER(TRIM(COALESCE(b.id_sku,'')))
               ORDER BY b.snapshot_ts DESC, b.run_id DESC
             ) rn
      FROM database-sigma.BRONZE_DB.bronze_affiliate b
    )
    WHERE rn = 1
  ),

  base AS (
    SELECT
      DATE(tanggal)                     AS tanggal,
      UPPER(TRIM(toko))                 AS toko,
      UPPER(TRIM(platform))             AS platform,
      UPPER(TRIM(id_pesanan))           AS id_pesanan,
      UPPER(TRIM(id_produk))            AS id_produk,
      UPPER(TRIM(produk))               AS produk,
      UPPER(TRIM(sku))                  AS sku,
      UPPER(TRIM(id_sku))               AS id_sku,
      UPPER(TRIM(penjual_sku))          AS penjual_sku,
      UPPER(TRIM(mata_uang))            AS mata_uang,

      SAFE_CAST(harga AS NUMERIC)            AS harga,
      SAFE_CAST(payment_amount AS NUMERIC)   AS payment_amount,
      SAFE_CAST(kuantitas AS INT64)          AS kuantitas,

      UPPER(TRIM(metode_pembayaran))         AS metode_pembayaran,
      UPPER(TRIM(status_pesanan))            AS status_pesanan,
      UPPER(TRIM(nama_pengguna_kreator))     AS nama_pengguna_kreator,
      UPPER(TRIM(jenis_konten))              AS jenis_konten,
      UPPER(TRIM(id_konten))                 AS id_konten,
      UPPER(TRIM(commission_model))          AS commission_model,

      SAFE_CAST(persentase_komisi_standar AS FLOAT64)/100             AS persen_komisi_std,
      SAFE_CAST(est_acuan_komisi AS NUMERIC)                          AS est_acuan_komisi,
      SAFE_CAST(perkiraan_pembayaran_komisi_standar AS NUMERIC)       AS perkiraan_bayar_komisi_std,
      SAFE_CAST(acuan_komisi_aktual AS NUMERIC)                       AS acuan_komisi_aktual,
      SAFE_CAST(pembayaran_komisi_aktual AS NUMERIC)                  AS pembayaran_komisi_aktual,
      SAFE_CAST(persentase_komisi_iklan_belanja AS FLOAT64)/100       AS persen_komisi_iklan_belanja,
      SAFE_CAST(perkiraan_pembayaran_komisi_iklan_belanja AS NUMERIC) AS perkiraan_bayar_komisi_iklan_belanja,
      SAFE_CAST(pembayaran_komisi_iklan_belanja_aktual AS NUMERIC)    AS bayar_komisi_iklan_belanja_aktual,
      SAFE_CAST(perkiraan_bonus_yang_ditanggung_bersama_untuk_kreator AS NUMERIC) AS perkiraan_bonus_ditanggungkan,
      SAFE_CAST(bonus_sebenarnya_yang_ditanggung_bersama_untuk_kreator AS NUMERIC) AS bonus_sebenarnya_ditanggungkan,

      SAFE_CAST(pengembalian_barang AS INT64) AS pengembalian_barang,
      SAFE_CAST(pengembalian_dana AS INT64)   AS pengembalian_dana,

      waktu_dibuat,
      waktu_pembayaran,
      waktu_pesanan_siap_dikirim,
      order_delivery_time,
      waktu_pesanan_selesai,
      waktu_komisi_dibayar,

      snapshot_ts, snapshot_date, run_id, row_hash_raw
    FROM latest_raw
  ),

  with_hash AS (
    SELECT
      b.*,
      SHA256(
        array_to_string([
          FORMAT_DATE('%F', b.tanggal), b.toko, COALESCE(b.id_pesanan,''), COALESCE(b.id_produk,''), COALESCE(b.id_sku,''),
          COALESCE(b.platform,''), COALESCE(b.produk,''), COALESCE(b.sku,''), COALESCE(b.penjual_sku,''), COALESCE(b.mata_uang,''),
          CAST(b.harga AS STRING), CAST(b.payment_amount AS STRING), CAST(b.kuantitas AS STRING),
          b.metode_pembayaran, b.status_pesanan, b.nama_pengguna_kreator,
          b.jenis_konten, b.id_konten, b.commission_model,
          CAST(b.persen_komisi_std AS STRING), CAST(b.est_acuan_komisi AS STRING),
          CAST(b.perkiraan_bayar_komisi_std AS STRING), CAST(b.acuan_komisi_aktual AS STRING),
          CAST(b.pembayaran_komisi_aktual AS STRING), CAST(b.persen_komisi_iklan_belanja AS STRING),
          CAST(b.perkiraan_bayar_komisi_iklan_belanja AS STRING), CAST(b.bayar_komisi_iklan_belanja_aktual AS STRING),
          CAST(b.perkiraan_bonus_ditanggungkan AS STRING), CAST(b.bonus_sebenarnya_ditanggungkan AS STRING),
          CAST(b.pengembalian_barang AS STRING), CAST(b.pengembalian_dana AS STRING),
          CAST(b.waktu_dibuat AS STRING), CAST(b.waktu_pembayaran AS STRING),
          CAST(b.waktu_pesanan_siap_dikirim AS STRING), CAST(b.order_delivery_time AS STRING),
          CAST(b.waktu_pesanan_selesai AS STRING), CAST(b.waktu_komisi_dibayar AS STRING)
        ], '||')
      ) AS row_hash_clean
    FROM base b
  )
  SELECT * FROM with_hash
) S
ON  T.tanggal = S.tanggal
AND T.toko = S.toko
AND COALESCE(T.id_pesanan,'') = COALESCE(S.id_pesanan,'')
AND COALESCE(T.id_produk ,'') = COALESCE(S.id_produk ,'')
AND COALESCE(T.id_sku    ,'') = COALESCE(S.id_sku    ,'')
WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
  UPDATE SET
    platform=S.platform, produk=S.produk, sku=S.sku, id_sku=S.id_sku, penjual_sku=S.penjual_sku, mata_uang=S.mata_uang,
    harga=S.harga, payment_amount=S.payment_amount, kuantitas=S.kuantitas,
    metode_pembayaran=S.metode_pembayaran, status_pesanan=S.status_pesanan,
    nama_pengguna_kreator=S.nama_pengguna_kreator, jenis_konten=S.jenis_konten, id_konten=S.id_konten, commission_model=S.commission_model,
    persen_komisi_std=S.persen_komisi_std, est_acuan_komisi=S.est_acuan_komisi,
    perkiraan_bayar_komisi_std=S.perkiraan_bayar_komisi_std, acuan_komisi_aktual=S.acuan_komisi_aktual, pembayaran_komisi_aktual=S.pembayaran_komisi_aktual,
    persen_komisi_iklan_belanja=S.persen_komisi_iklan_belanja, perkiraan_bayar_komisi_iklan_belanja=S.perkiraan_bayar_komisi_iklan_belanja,
    bayar_komisi_iklan_belanja_aktual=S.bayar_komisi_iklan_belanja_aktual,
    perkiraan_bonus_ditanggungkan=S.perkiraan_bonus_ditanggungkan, bonus_sebenarnya_ditanggungkan=S.bonus_sebenarnya_ditanggungkan,
    pengembalian_barang=S.pengembalian_barang, pengembalian_dana=S.pengembalian_dana,
    waktu_dibuat=S.waktu_dibuat, waktu_pembayaran=S.waktu_pembayaran, waktu_pesanan_siap_dikirim=S.waktu_pesanan_siap_dikirim,
    order_delivery_time=S.order_delivery_time, waktu_pesanan_selesai=S.waktu_pesanan_selesai, waktu_komisi_dibayar=S.waktu_komisi_dibayar,
    snapshot_ts=S.snapshot_ts, snapshot_date=S.snapshot_date, run_id=S.run_id,
    row_hash_raw=S.row_hash_raw, row_hash_clean=S.row_hash_clean
WHEN NOT MATCHED THEN
  INSERT (
    tanggal, toko, platform, id_pesanan, id_produk, produk, sku, id_sku, penjual_sku, mata_uang,
    harga, payment_amount, kuantitas, metode_pembayaran, status_pesanan,
    nama_pengguna_kreator, jenis_konten, id_konten, commission_model,
    persen_komisi_std, est_acuan_komisi, perkiraan_bayar_komisi_std, acuan_komisi_aktual,
    pembayaran_komisi_aktual, persen_komisi_iklan_belanja, perkiraan_bayar_komisi_iklan_belanja,
    bayar_komisi_iklan_belanja_aktual, perkiraan_bonus_ditanggungkan, bonus_sebenarnya_ditanggungkan,
    pengembalian_barang, pengembalian_dana,
    waktu_dibuat, waktu_pembayaran, waktu_pesanan_siap_dikirim, order_delivery_time, waktu_pesanan_selesai, waktu_komisi_dibayar,
    snapshot_ts, snapshot_date, run_id, row_hash_raw, row_hash_clean
  )
  VALUES (
    S.tanggal, S.toko, S.platform, S.id_pesanan, S.id_produk, S.produk, S.sku, S.id_sku, S.penjual_sku, S.mata_uang,
    S.harga, S.payment_amount, S.kuantitas, S.metode_pembayaran, S.status_pesanan,
    S.nama_pengguna_kreator, S.jenis_konten, S.id_konten, S.commission_model,
    S.persen_komisi_std, S.est_acuan_komisi, S.perkiraan_bayar_komisi_std, S.acuan_komisi_aktual,
    S.pembayaran_komisi_aktual, S.persen_komisi_iklan_belanja, S.perkiraan_bayar_komisi_iklan_belanja,
    S.bayar_komisi_iklan_belanja_aktual, S.perkiraan_bonus_ditanggungkan, S.bonus_sebenarnya_ditanggungkan,
    S.pengembalian_barang, S.pengembalian_dana,
    S.waktu_dibuat, S.waktu_pembayaran, S.waktu_pesanan_siap_dikirim, S.order_delivery_time, S.waktu_pesanan_selesai, S.waktu_komisi_dibayar,
    S.snapshot_ts, S.snapshot_date, S.run_id, S.row_hash_raw, S.row_hash_clean
  );

-- MERGE `database-sigma.SILVER_DB.silver_tt_affiliate` T
-- USING (
--   WITH latest_raw AS (
--     SELECT * EXCEPT(rn) FROM (
--       SELECT b.*,
--              ROW_NUMBER() OVER (
--                PARTITION BY UPPER(TRIM(b.toko)),
--                             DATE(b.tanggal),
--                             UPPER(TRIM(COALESCE(b.id_pesanan,''))),
--                             UPPER(TRIM(COALESCE(b.id_produk,''))),
--                             UPPER(TRIM(COALESCE(b.id_sku,'')))
--                ORDER BY b.snapshot_ts DESC, b.run_id DESC
--              ) rn
--       FROM `database-sigma.BRONZE_DB.bronze_affiliate` b
--     )
--     WHERE rn = 1
--   ),

--   base AS (
--     SELECT
--       DATE(tanggal)                     AS tanggal,
--       UPPER(TRIM(toko))                 AS toko,
--       UPPER(TRIM(platform))             AS platform,
--       UPPER(TRIM(id_pesanan))           AS id_pesanan,
--       UPPER(TRIM(id_produk))            AS id_produk,
--       UPPER(TRIM(produk))               AS produk,
--       UPPER(TRIM(sku))                  AS sku,
--       UPPER(TRIM(id_sku))               AS id_sku,
--       UPPER(TRIM(penjual_sku))          AS penjual_sku,
--       UPPER(TRIM(mata_uang))            AS mata_uang,

--       SAFE_CAST(harga AS NUMERIC)            AS harga,
--       SAFE_CAST(payment_amount AS NUMERIC)   AS payment_amount,
--       SAFE_CAST(kuantitas AS INT64)          AS kuantitas,

--       UPPER(TRIM(metode_pembayaran))         AS metode_pembayaran,
--       UPPER(TRIM(status_pesanan))            AS status_pesanan,
--       UPPER(TRIM(nama_pengguna_kreator))     AS nama_pengguna_kreator,
--       UPPER(TRIM(jenis_konten))              AS jenis_konten,
--       UPPER(TRIM(id_konten))                 AS id_konten,
--       UPPER(TRIM(commission_model))          AS commission_model,

--       SAFE_CAST(persentase_komisi_standar AS FLOAT64)/100             AS persen_komisi_std,
--       SAFE_CAST(est_acuan_komisi AS NUMERIC)                          AS est_acuan_komisi,
--       SAFE_CAST(perkiraan_pembayaran_komisi_standar AS NUMERIC)       AS perkiraan_bayar_komisi_std,
--       SAFE_CAST(acuan_komisi_aktual AS NUMERIC)                       AS acuan_komisi_aktual,
--       SAFE_CAST(pembayaran_komisi_aktual AS NUMERIC)                  AS pembayaran_komisi_aktual,
--       SAFE_CAST(persentase_komisi_iklan_belanja AS FLOAT64)/100       AS persen_komisi_iklan_belanja,
--       SAFE_CAST(perkiraan_pembayaran_komisi_iklan_belanja AS NUMERIC) AS perkiraan_bayar_komisi_iklan_belanja,
--       SAFE_CAST(pembayaran_komisi_iklan_belanja_aktual AS NUMERIC)    AS bayar_komisi_iklan_belanja_aktual,
--       SAFE_CAST(perkiraan_bonus_yang_ditanggung_bersama_untuk_kreator AS NUMERIC) AS perkiraan_bonus_ditanggungkan,
--       SAFE_CAST(bonus_sebenarnya_yang_ditanggung_bersama_untuk_kreator AS NUMERIC) AS bonus_sebenarnya_ditanggungkan,

--       SAFE_CAST(pengembalian_barang AS INT64) AS pengembalian_barang,
--       SAFE_CAST(pengembalian_dana AS INT64)   AS pengembalian_dana,

--       waktu_dibuat,
--       waktu_pembayaran,
--       waktu_pesanan_siap_dikirim,
--       order_delivery_time,
--       waktu_pesanan_selesai,
--       waktu_komisi_dibayar,

--       snapshot_ts, snapshot_date, run_id, row_hash_raw
--     FROM latest_raw
--   ),

--   with_hash AS (
--     SELECT
--       b.*,
--       TO_HEX(SHA256(
--         ARRAY_TO_STRING([
--           FORMAT_DATE('%F', b.tanggal), b.toko, COALESCE(b.id_pesanan,''), COALESCE(b.id_produk,''), COALESCE(b.id_sku,''),
--           COALESCE(b.platform,''), COALESCE(b.produk,''), COALESCE(b.sku,''), COALESCE(b.penjual_sku,''), COALESCE(b.mata_uang,''),
--           CAST(b.harga AS STRING), CAST(b.payment_amount AS STRING), CAST(b.kuantitas AS STRING),
--           b.metode_pembayaran, b.status_pesanan, b.nama_pengguna_kreator,
--           b.jenis_konten, b.id_konten, b.commission_model,
--           CAST(b.persen_komisi_std AS STRING), CAST(b.est_acuan_komisi AS STRING),
--           CAST(b.perkiraan_bayar_komisi_std AS STRING), CAST(b.acuan_komisi_aktual AS STRING),
--           CAST(b.pembayaran_komisi_aktual AS STRING), CAST(b.persen_komisi_iklan_belanja AS STRING),
--           CAST(b.perkiraan_bayar_komisi_iklan_belanja AS STRING), CAST(b.bayar_komisi_iklan_belanja_aktual AS STRING),
--           CAST(b.perkiraan_bonus_ditanggungkan AS STRING), CAST(b.bonus_sebenarnya_ditanggungkan AS STRING),
--           CAST(b.pengembalian_barang AS STRING), CAST(b.pengembalian_dana AS STRING),
--           CAST(b.waktu_dibuat AS STRING), CAST(b.waktu_pembayaran AS STRING),
--           CAST(b.waktu_pesanan_siap_dikirim AS STRING), CAST(b.order_delivery_time AS STRING),
--           CAST(b.waktu_pesanan_selesai AS STRING), CAST(b.waktu_komisi_dibayar AS STRING)
--         ], '||')
--       )) AS row_hash_clean
--     FROM base b
--   )
--   SELECT * FROM with_hash
-- ) S
-- ON  T.tanggal = S.tanggal
-- AND T.toko = S.toko
-- AND COALESCE(T.id_pesanan,'') = COALESCE(S.id_pesanan,'')
-- AND COALESCE(T.id_produk ,'') = COALESCE(S.id_produk ,'')
-- AND COALESCE(T.id_sku    ,'') = COALESCE(S.id_sku    ,'')
-- WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
--   UPDATE SET
--     platform=S.platform, produk=S.produk, sku=S.sku, id_sku=S.id_sku, penjual_sku=S.penjual_sku, mata_uang=S.mata_uang,
--     harga=S.harga, payment_amount=S.payment_amount, kuantitas=S.kuantitas,
--     metode_pembayaran=S.metode_pembayaran, status_pesanan=S.status_pesanan,
--     nama_pengguna_kreator=S.nama_pengguna_kreator, jenis_konten=S.jenis_konten, id_konten=S.id_konten, commission_model=S.commission_model,
--     persen_komisi_std=S.persen_komisi_std, est_acuan_komisi=S.est_acuan_komisi,
--     perkiraan_bayar_komisi_std=S.perkiraan_bayar_komisi_std, acuan_komisi_aktual=S.acuan_komisi_aktual, pembayaran_komisi_aktual=S.pembayaran_komisi_aktual,
--     persen_komisi_iklan_belanja=S.persen_komisi_iklan_belanja, perkiraan_bayar_komisi_iklan_belanja=S.perkiraan_bayar_komisi_iklan_belanja,
--     bayar_komisi_iklan_belanja_aktual=S.bayar_komisi_iklan_belanja_aktual,
--     perkiraan_bonus_ditanggungkan=S.perkiraan_bonus_ditanggungkan, bonus_sebenarnya_ditanggungkan=S.bonus_sebenarnya_ditanggungkan,
--     pengembalian_barang=S.pengembalian_barang, pengembalian_dana=S.pengembalian_dana,
--     waktu_dibuat=S.waktu_dibuat, waktu_pembayaran=S.waktu_pembayaran, waktu_pesanan_siap_dikirim=S.waktu_pesanan_siap_dikirim,
--     order_delivery_time=S.order_delivery_time, waktu_pesanan_selesai=S.waktu_pesanan_selesai, waktu_komisi_dibayar=S.waktu_komisi_dibayar,
--     snapshot_ts=S.snapshot_ts, snapshot_date=S.snapshot_date, run_id=S.run_id,
--     row_hash_raw=S.row_hash_raw, row_hash_clean=S.row_hash_clean
-- WHEN NOT MATCHED THEN
--   INSERT (
--     tanggal, toko, platform, id_pesanan, id_produk, produk, sku, id_sku, penjual_sku, mata_uang,
--     harga, payment_amount, kuantitas, metode_pembayaran, status_pesanan,
--     nama_pengguna_kreator, jenis_konten, id_konten, commission_model,
--     persen_komisi_std, est_acuan_komisi, perkiraan_bayar_komisi_std, acuan_komisi_aktual,
--     pembayaran_komisi_aktual, persen_komisi_iklan_belanja, perkiraan_bayar_komisi_iklan_belanja,
--     bayar_komisi_iklan_belanja_aktual, perkiraan_bonus_ditanggungkan, bonus_sebenarnya_ditanggungkan,
--     pengembalian_barang, pengembalian_dana,
--     waktu_dibuat, waktu_pembayaran, waktu_pesanan_siap_dikirim, order_delivery_time, waktu_pesanan_selesai, waktu_komisi_dibayar,
--     snapshot_ts, snapshot_date, run_id, row_hash_raw, row_hash_clean
--   )
--   VALUES (
--     S.tanggal, S.toko, S.platform, S.id_pesanan, S.id_produk, S.produk, S.sku, S.id_sku, S.penjual_sku, S.mata_uang,
--     S.harga, S.payment_amount, S.kuantitas, S.metode_pembayaran, S.status_pesanan,
--     S.nama_pengguna_kreator, S.jenis_konten, S.id_konten, S.commission_model,
--     S.persen_komisi_std, S.est_acuan_komisi, S.perkiraan_bayar_komisi_std, S.acuan_komisi_aktual,
--     S.pembayaran_komisi_aktual, S.persen_komisi_iklan_belanja, S.perkiraan_bayar_komisi_iklan_belanja,
--     S.bayar_komisi_iklan_belanja_aktual, S.perkiraan_bonus_ditanggungkan, S.bonus_sebenarnya_ditanggungkan,
--     S.pengembalian_barang, S.pengembalian_dana,
--     S.waktu_dibuat, S.waktu_pembayaran, S.waktu_pesanan_siap_dikirim, S.order_delivery_time, S.waktu_pesanan_selesai, S.waktu_komisi_dibayar,
--     S.snapshot_ts, S.snapshot_date, S.run_id, S.row_hash_raw, S.row_hash_clean
--   );