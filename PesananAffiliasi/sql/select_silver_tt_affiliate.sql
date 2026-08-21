SELECT
  * REPLACE (UPPER(TRIM(nama_pengguna_kreator)) AS creator_username)
FROM `database-sigma.Testing.silver_tt_affiliate`;