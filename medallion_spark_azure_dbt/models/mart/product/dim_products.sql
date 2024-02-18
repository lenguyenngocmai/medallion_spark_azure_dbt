{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/gold/products"
    )
}}

WITH stg__saleslt__product AS (
    SELECT *
    FROM {{ ref('stg__saleslt__product') }}
    WHERE dbt_valid_to IS NULL 
)

, stg__saleslt__productmodel AS (
    SELECT *
    FROM {{ ref('stg__saleslt__productmodel') }}
    WHERE dbt_valid_to IS NULL 
)

SELECT 
    product.product_id
    , product.product_name
    , product.product_number
    , product.product_category_id
    , product.product_model_id
    , COALESCE(productmodel.product_model_name, "Undefined") AS product_model_name  
    , COALESCE(productmodel.catalog_description, "Undefined") AS catalog_description  
    , product.standard_cost
    , product.list_price
    , product.product_color
    , product.product_size
    , product.product_weight
    , product.sell_start_date
    , product.sell_end_date
    , product.discontinued_date

FROM stg__saleslt__product AS product
LEFT JOIN stg__saleslt__productmodel AS productmodel
    USING(product_model_id)