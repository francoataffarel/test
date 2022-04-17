SELECT atr.brand_id                                          AS brand_id,
       atr.brand                                             AS brand,
       atr.category                                          AS category,
       atr.sub_category                                      AS sub_category,
       atr.channel                                           AS channel,
       atr.key                                               AS attr_key,
       Avg(Cast(atr.value AS DECIMAL(10, 2)))                AS attr_value,
       Count(atr.key)                                        AS attr_frequency,
       To_timestamp(atr.year
                    || '-'
                    || atr.month
                    || '-'
                    || atr.day
                    || ' 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS ts,
       atr.year,
       atr.month,
       atr.day,
       CURRENT_DATE as current_date
FROM   rnr_spectrum_prod.attribute atr
GROUP  BY atr.brand,
          atr.brand_id,
          atr.category,
          atr.sub_category,
          atr.channel,
          atr.key,
          atr.year,
          atr.month,
          atr.day
