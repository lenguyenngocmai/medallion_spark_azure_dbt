{{config(
      file_format = "delta",
      location_root = "/mnt/silver/salesorderdetail",
)}}  


WITH  renamed_column AS (
      SELECT 
            SalesOrderID AS sales_order_id
            , SalesOrderDetailID AS sales_order_detail_id
            , OrderQty AS ordered_quantity
            , ProductID AS product_id
            , UnitPrice AS unit_price
            , UnitPriceDiscount AS unit_price_discount
            , LineTotal AS net_amount
            , dbt_valid_from
            , dbt_valid_to
      FROM {{ ref('salesorderdetail_snapshot') }}
)


, cast_type AS (
      SELECT 
            sales_order_id::TEXT
            , sales_order_detail_id::TEXT
            , product_id::TEXT
            , ordered_quantity::NUMERIC
            , unit_price::NUMERIC
            , unit_price_discount::NUMERIC
            , net_amount::NUMERIC
            , dbt_valid_from::TIMESTAMPTZ
            , dbt_valid_to::TIMESTAMPTZ
      FROM renamed_column
)

SELECT 
      sales_order_id
      , sales_order_detail_id
      , product_id
      , ordered_quantity
      , unit_price
      , unit_price_discount
      , net_amount
      , dbt_valid_from
      , dbt_valid_to
FROM cast_type