{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/gold/sales"
    )
}}


WITH sales_header AS (
    SELECT *
    FROM {{ ref('stg__saleslt__salesorderheader') }}
    WHERE dbt_valid_to IS NOT NULL
)

, sales_detail AS (
    SELECT *
    FROM {{ ref('stg__saleslt__salesorderdetail') }}
    WHERE dbt_valid_to IS NOT NULL
)

SELECT 
    --SYSTEM KEY
    sales_detail.sales_order_detail_id
    , sales_detail.sales_order_id
    , sales_detail.product_id
    , sales_header.customer_id

    --NATURAL KEY
    , sales_header.sales_order_number
    , sales_header.purchase_order_number
    , sales_header.account_number

    --DATE
    , sales_header.order_date
    , sales_header.due_date
    , sales_header.ship_date

    --INFO
    , sales_header.order_status  
    , sales_header.is_online_order

    --AMOUNT
    , sales_detail.ordered_quantity
    , sales_detail.unit_price
    , sales_detail.unit_price_discount
    , sales_detail.net_amount

FROM sales_detail
JOIN sales_header
    USING(sales_order_id)