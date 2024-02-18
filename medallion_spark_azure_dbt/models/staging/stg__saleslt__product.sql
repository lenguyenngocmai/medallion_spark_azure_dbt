{{config(
      file_format = "delta",
      location_root = "/mnt/silver/product",
)}}  

WITH renamed_column AS (
    SELECT 
        ProductID AS product_id
        , Name AS product_name
        , ProductNumber AS product_number
        , ProductCategoryID AS product_category_id
        , ProductModelID AS product_model_id

        , StandardCost AS standard_cost
        , ListPrice AS list_price
        , Color AS product_color
        , Size AS product_size
        , Weight AS product_weight

        , SellStartDate AS sell_start_date 
        , SellEndDate AS sell_end_date
        , DiscontinuedDate AS discontinued_date
        , dbt_valid_to
        , dbt_valid_from
        
    FROM {{ ref('product_snapshot') }}
)

, cast_type AS (
    SELECT 
       product_id::TEXT
        , product_name::TEXT
        , product_number::TEXT
        , product_category_id::TEXT
        , product_model_id::TEXT

        , standard_cost::NUMERIC
        , list_price::NUMERIC
        , product_color::TEXT
        , product_size::TEXT
        , product_weight::TEXT

        , sell_start_date::DATE
        , sell_end_date::DATE
        , discontinued_date::DATE
        , dbt_valid_to::TIMESTAMPTZ
        , dbt_valid_from::TIMESTAMPTZ        
    FROM renamed_column
)

SELECT 
    product_id
    , product_name
    , product_number
    , product_category_id
    , product_model_id
    , standard_cost
    , list_price
    , product_color
    , product_size
    , product_weight
    , sell_start_date
    , sell_end_date
    , discontinued_date
    , dbt_valid_to
    , dbt_valid_from
FROM cast_type