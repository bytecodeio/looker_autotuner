WITH product_comparison AS (WITH min_order_date_per_order_id AS (SELECT as03a9a92583a.*,
       min(created_date) over (partition by order_id)  AS min_created,
min(created_date) over (partition by user_id)  AS min_created_user
FROM (SELECT
    (TIMESTAMP_TRUNC(order_items.created_at , DAY)) AS created_date,
    order_items.order_id  AS order_id,
    order_items.user_id  AS user_id
FROM `looker-partners.thelook.order_items`
     AS order_items
WHERE ((DATE(order_items.created_at )) <= CURRENT_DATE() )
GROUP BY
    1,
    2,
    3) as03a9a92583a
)
SELECT
    t6.`__f10` AS min_created_date,
    t6.`__f1` AS brand,
    t6.`__f2` AS category,
        CASE WHEN COUNT(CASE WHEN CASE WHEN t6.groupingVal IN (0, 1) THEN 0 ELSE 16 END + CASE WHEN t6.groupingVal IN (0, 1) THEN 0 ELSE 8 END + CASE WHEN t6.groupingVal IN (0, 1) THEN 0 ELSE 4 END + CASE WHEN t6.groupingVal = 0 THEN 0 ELSE 2 END + CASE WHEN t6.groupingVal = 1 THEN 0 ELSE 1 END = 2 AND t6.`__f7` > 0 THEN t6.`__f6` ELSE NULL END) = 0 THEN NULL ELSE COALESCE(SUM(CASE WHEN CASE WHEN t6.groupingVal IN (0, 1) THEN 0 ELSE 16 END + CASE WHEN t6.groupingVal IN (0, 1) THEN 0 ELSE 8 END + CASE WHEN t6.groupingVal IN (0, 1) THEN 0 ELSE 4 END + CASE WHEN t6.groupingVal = 0 THEN 0 ELSE 2 END + CASE WHEN t6.groupingVal = 1 THEN 0 ELSE 1 END = 2 AND t6.`__f7` > 0 THEN t6.`__f6` ELSE NULL END), 0) END AS total_gross_revenue,
    COUNT(CASE WHEN CASE WHEN t6.groupingVal IN (0, 1) THEN 0 ELSE 16 END + CASE WHEN t6.groupingVal IN (0, 1) THEN 0 ELSE 8 END + CASE WHEN t6.groupingVal IN (0, 1) THEN 0 ELSE 4 END + CASE WHEN t6.groupingVal = 0 THEN 0 ELSE 2 END + CASE WHEN t6.groupingVal = 1 THEN 0 ELSE 1 END = 1 THEN t6.`__f8` ELSE NULL END) AS count_of_order_items
FROM
    (SELECT
            CASE WHEN t4.groupingVal IN (0, 1) THEN t1.brand ELSE NULL END AS `__f1`,
                CASE WHEN t4.groupingVal IN (0, 1) THEN t1.category ELSE NULL END AS `__f2`,
                CASE WHEN t4.groupingVal = 0 THEN t1.order_items_id ELSE NULL END AS `__f3`,
                CASE WHEN t4.groupingVal IN (0, 1) THEN t1.min_created_date ELSE NULL END AS `__f10`,
                CASE WHEN t4.groupingVal = 1 THEN t1.`__f13` ELSE NULL END AS `__f13`,
            t4.groupingVal,
            MIN(CASE WHEN t1.`__f12` THEN t1.order_items_sale_price ELSE NULL END) AS `__f6`,
            COUNT(CASE WHEN t1.`__f12` THEN 1 ELSE NULL END) AS `__f7`,
            MIN(t1.order_items_id) AS `__f8`
        FROM
            (SELECT
                    products.brand  AS brand,
                    products.category  AS category,
                    order_items.id  AS order_items_id,
                    order_items.sale_price  AS order_items_sale_price,
                        (TIMESTAMP_TRUNC(min_order_date_per_order_id.min_created, DAY)) AS min_created_date,
                        CASE WHEN order_items.status NOT IN  ("Returned","Cancelled") THEN TRUE ELSE FALSE END AS `__f12`,
                    order_items.id  AS `__f13`
                FROM `looker-partners.thelook.products`
     AS products
LEFT JOIN `looker-partners.thelook.order_items`
     AS order_items ON products.id = order_items.product_id
LEFT JOIN min_order_date_per_order_id ON order_items.order_id = min_order_date_per_order_id.order_id
                WHERE (products.brand is not null )) AS t1,
                (SELECT
                        0 AS groupingVal
                    UNION ALL
                    SELECT
                        1 AS groupingVal) AS t4
        GROUP BY
            CASE WHEN t4.groupingVal IN (0, 1) THEN t1.brand ELSE NULL END,
            CASE WHEN t4.groupingVal IN (0, 1) THEN t1.category ELSE NULL END,
            CASE WHEN t4.groupingVal = 0 THEN t1.order_items_id ELSE NULL END,
            CASE WHEN t4.groupingVal IN (0, 1) THEN t1.min_created_date ELSE NULL END,
            CASE WHEN t4.groupingVal = 1 THEN t1.`__f13` ELSE NULL END,
            t4.groupingVal) AS t6
GROUP BY
    1,
    2,
    3)
  ,  min_order_date_per_order_id AS (SELECT as03a9a92583a.*,
       min(created_date) over (partition by order_id)  AS min_created,
min(created_date) over (partition by user_id)  AS min_created_user
FROM (SELECT
    (TIMESTAMP_TRUNC(order_items.created_at , DAY)) AS created_date,
    order_items.order_id  AS order_id,
    order_items.user_id  AS user_id
FROM `looker-partners.thelook.order_items`
     AS order_items
WHERE ((DATE(order_items.created_at )) <= CURRENT_DATE() )
GROUP BY
    1,
    2,
    3) as03a9a92583a
)
  ,  average_gross_margin_percentage_roll_up AS (WITH min_order_date_per_order_id AS (SELECT as03a9a92583a.*,
       min(created_date) over (partition by order_id)  AS min_created,
min(created_date) over (partition by user_id)  AS min_created_user
FROM (SELECT
    (TIMESTAMP_TRUNC(order_items.created_at , DAY)) AS created_date,
    order_items.order_id  AS order_id,
    order_items.user_id  AS user_id
FROM `looker-partners.thelook.order_items`
     AS order_items
WHERE ((DATE(order_items.created_at )) <= CURRENT_DATE() )
GROUP BY
    1,
    2,
    3) as03a9a92583a
)
SELECT
    (TIMESTAMP_TRUNC(min_order_date_per_order_id.min_created, DAY)) AS min_created_date,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ( order_items.status NOT IN  ("Returned","Cancelled"))  THEN  order_items.sale_price   ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( order_items.status NOT IN  ("Returned","Cancelled"))  THEN  order_items.id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( order_items.status NOT IN  ("Returned","Cancelled"))  THEN  order_items.id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( order_items.status NOT IN  ("Returned","Cancelled"))  THEN  order_items.id   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( order_items.status NOT IN  ("Returned","Cancelled"))  THEN  order_items.id   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS total_gross_revenue,
        ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (order_items.sale_price )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (order_items.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (order_items.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (order_items.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (order_items.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) - ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (inventory_items.cost )  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (order_items.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (order_items.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (order_items.id )  ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  (( order_items.status NOT IN  ("Returned","Cancelled")))  THEN  (order_items.id )  ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS total_gross_margin_amount
FROM `looker-partners.thelook.products`
     AS products
LEFT JOIN `looker-partners.thelook.order_items`
     AS order_items ON products.id = order_items.product_id
LEFT JOIN min_order_date_per_order_id ON order_items.order_id = min_order_date_per_order_id.order_id
LEFT JOIN `looker-partners.thelook.inventory_items`
     AS inventory_items ON products.id = inventory_items.product_id

WHERE ((( min_order_date_per_order_id.min_created ) >= (TIMESTAMP('2022-01-01 00:00:00')) AND ( min_order_date_per_order_id.min_created ) < (TIMESTAMP('2023-01-01 00:00:00')))) AND (products.brand is not null )
GROUP BY
    1)
SELECT
    products.brand  AS products_brand,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  ( product_comparison.brand='Ray-Ban' )  THEN  product_comparison.total_gross_revenue  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( product_comparison.brand='Ray-Ban' )  THEN  concat(product_comparison.brand,'|',product_comparison.category,'|', (DATE(product_comparison.min_created_date, 'America/Los_Angeles')))   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( product_comparison.brand='Ray-Ban' )  THEN  concat(product_comparison.brand,'|',product_comparison.category,'|', (DATE(product_comparison.min_created_date, 'America/Los_Angeles')))   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( product_comparison.brand='Ray-Ban' )  THEN  concat(product_comparison.brand,'|',product_comparison.category,'|', (DATE(product_comparison.min_created_date, 'America/Los_Angeles')))   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  ( product_comparison.brand='Ray-Ban' )  THEN  concat(product_comparison.brand,'|',product_comparison.category,'|', (DATE(product_comparison.min_created_date, 'America/Los_Angeles')))   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS product_comparison_desired_metric_measure_base,
    ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN  NOT COALESCE(( product_comparison.brand='Ray-Ban'  ), FALSE)  THEN  product_comparison.total_gross_revenue  ELSE NULL END
,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  NOT COALESCE(( product_comparison.brand='Ray-Ban'  ), FALSE)  THEN  concat(product_comparison.brand,'|',product_comparison.category,'|', (DATE(product_comparison.min_created_date, 'America/Los_Angeles')))   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  NOT COALESCE(( product_comparison.brand='Ray-Ban'  ), FALSE)  THEN  concat(product_comparison.brand,'|',product_comparison.category,'|', (DATE(product_comparison.min_created_date, 'America/Los_Angeles')))   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  NOT COALESCE(( product_comparison.brand='Ray-Ban'  ), FALSE)  THEN  concat(product_comparison.brand,'|',product_comparison.category,'|', (DATE(product_comparison.min_created_date, 'America/Los_Angeles')))   ELSE NULL END
 AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(CASE WHEN  NOT COALESCE(( product_comparison.brand='Ray-Ban'  ), FALSE)  THEN  concat(product_comparison.brand,'|',product_comparison.category,'|', (DATE(product_comparison.min_created_date, 'America/Los_Angeles')))   ELSE NULL END
 AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS product_comparison_desired_metric_measure_compare,
    1.0* ( (ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN ( order_items.status NOT IN  ("Returned","Cancelled") ) THEN order_items.sale_price  ELSE NULL END,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6)) - (ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN ( order_items.status NOT IN  ("Returned","Cancelled") ) THEN inventory_items.cost ELSE NULL END,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6))  ) / nullif(( ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(CASE WHEN ( order_items.status NOT IN  ("Returned","Cancelled") ) THEN order_items.sale_price  ELSE NULL END,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(order_items.id  AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) ),0)  AS products_cross_view_gross_margin_percentage,
    1.0* ( ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(average_gross_margin_percentage_roll_up.total_gross_margin_amount ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(average_gross_margin_percentage_roll_up.min_created_date AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(average_gross_margin_percentage_roll_up.min_created_date AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(average_gross_margin_percentage_roll_up.min_created_date AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(average_gross_margin_percentage_roll_up.min_created_date AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) ) / nullif(( ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE(average_gross_margin_percentage_roll_up.total_gross_revenue ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST(average_gross_margin_percentage_roll_up.min_created_date AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(average_gross_margin_percentage_roll_up.min_created_date AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST(average_gross_margin_percentage_roll_up.min_created_date AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST(average_gross_margin_percentage_roll_up.min_created_date AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) ),0)  AS average_gross_margin_percentage_roll_up_average_gross_margin_percentage_rolled_up
FROM `looker-partners.thelook.products`
     AS products
LEFT JOIN `looker-partners.thelook.order_items`
     AS order_items ON products.id = order_items.product_id
LEFT JOIN min_order_date_per_order_id ON order_items.order_id = min_order_date_per_order_id.order_id
LEFT JOIN product_comparison ON products.brand = product_comparison.brand and
             products.category = product_comparison.category and
             (DATE(min_order_date_per_order_id.min_created, 'America/Los_Angeles')) = (DATE(product_comparison.min_created_date, 'America/Los_Angeles'))
LEFT JOIN `looker-partners.thelook.inventory_items`
     AS inventory_items ON products.id = inventory_items.product_id

LEFT JOIN average_gross_margin_percentage_roll_up ON (DATE(min_order_date_per_order_id.min_created, 'America/Los_Angeles')) = (DATE(average_gross_margin_percentage_roll_up.min_created_date, 'America/Los_Angeles'))
WHERE ((( min_order_date_per_order_id.min_created ) >= (TIMESTAMP('2022-01-01 00:00:00', 'America/Los_Angeles')) AND ( min_order_date_per_order_id.min_created ) < (TIMESTAMP('2023-01-01 00:00:00', 'America/Los_Angeles')))) AND (products.brand is not null )
GROUP BY
    1
ORDER BY
    2 DESC,
    3 DESC
LIMIT 11