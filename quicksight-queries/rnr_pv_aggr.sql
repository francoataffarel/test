SELECT
       ranked.brand                              AS brand,
       ranked.brand_id                           AS brand_id,
       ranked.category                           AS category,
       ranked.sub_category                       AS sub_category,
       ranked.channel                            AS channel,
       ranked.skuid                              AS skuid,
       ranked.product_id                         AS product_id,
       ranked.name                               AS name,
       ranked.mrp                                AS mrp,
       ranked.price                              AS seller_price,
       ranked.rating_count                       AS rating_count,
       ranked.rating                             AS rating,
       ranked.best_seller_rank                   AS best_seller_rank,
       atr.key                                   AS attr_key,
       Avg(Cast(atr.value AS DECIMAL(10, 2))) AS attr_value,
       atr.year,
       atr.month,
       atr.day,
       to_timestamp(atr.year || '-' || atr.month  || '-' || atr.day  || ' 00:00:00', 'YYYY-MM-DD HH24:MI:SS') as ts,
      CURRENT_DATE as current_date
FROM   rnr_spectrum_prod.attribute atr
       join (SELECT id                                            AS
                    product_version_id
                    ,
                    brand,
                    brand_id,
                    category,
                    sub_category,
                    channel,
                    skuid,
                    product_id,
                    name,
                    mrp,
                    price,
                    rating_count,
                    rating,
                    best_seller_rank,
                    year,
                    month,
                    day,
                    Row_number()
                      over (
                        PARTITION BY skuid
                        ORDER BY year DESC, month DESC, day DESC) AS
                    skuid_ranked
             FROM   rnr_spectrum_prod.product_version) ranked
         ON ranked.product_version_id = atr.product_version_id
            AND ranked.skuid_ranked = 1
GROUP  BY ranked.brand,
          ranked.brand_id,
          ranked.category,
          ranked.sub_category,
          ranked.channel,
          ranked.skuid,
          ranked.product_id,
          ranked.name,
          ranked.mrp,
          ranked.price,
          ranked.rating_count,
          ranked.rating,
          ranked.best_seller_rank,
          atr.key,
          atr.year,
          atr.month,
          atr.day
