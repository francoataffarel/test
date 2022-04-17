SELECT
      pv.brand,
      pv.brand_id,
      pv.category,
      pv.sub_category,
      pv.channel,
      pv.year,
      pv.month,
      pv.day,
      to_timestamp(pv.year || '-' || pv.month || '-' || pv.day || ' 00:00:00', 'YYYY-MM-DD HH24:MI:SS') as ts,
      COUNT(pv.product_id) as b_prod_count,
      SUM(CAST(pv.rating_count AS BIGINT)) as b_rating_count,
      AVG(CAST(pv.rating AS DECIMAL(10, 2))) as b_avg_rating,
      CAST(SUM(CAST(pv.rating AS DECIMAL(10, 2))*CAST(pv.rating_count AS BIGINT))/b_rating_count AS DECIMAL(10, 2)) as b_wt_avg_rating,
      MAX(CAST(pv.price AS BIGINT)) as b_max_price,
      MIN(CAST(pv.price AS BIGINT)) as b_min_price,
      CURRENT_DATE as current_date
    from
      rnr_spectrum_prod.product_version pv
    group by
      pv.brand, pv.brand_id, pv.category, pv.sub_category, pv.channel, pv.year, pv.month, pv.day
