SELECT
  * REPLACE (UPPER(TRIM(nama_pengguna_kreator)) AS creator_username)
FROM `database-sigma.SILVER_DB.silver_tt_affiliate`;