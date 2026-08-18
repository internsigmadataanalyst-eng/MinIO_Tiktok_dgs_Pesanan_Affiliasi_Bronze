SELECT
    UPPER(TRIM(creator_username)) AS creator_username,
    ANY_VALUE(jenis_affiliate) AS jenis_affiliate,
    ANY_VALUE(status_affiliate) AS status_affiliate
  FROM `database-sigma.BRONZE_DB.bronze_akun_affiliate`
  GROUP BY creator_username