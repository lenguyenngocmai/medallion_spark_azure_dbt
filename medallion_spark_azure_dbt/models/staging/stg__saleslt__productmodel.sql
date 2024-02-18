{{config(
    file_format = "delta",
    location_root = "/mnt/silver/productmodel",
)}}

WITH renamed_column AS (
    SELECT
        ProductModelID AS product_model_id
        , Name AS product_model_name
        , CatalogDescription AS catalog_description
        , dbt_valid_to
        , dbt_valid_from
    FROM {{ ref('productmodel_snapshot') }}
)

, cast_type AS (
    SELECT 
        product_model_id::TEXT
        , product_model_name::TEXT 
        , catalog_description::TEXT 
        , dbt_valid_to::TIMESTAMPTZ
        , dbt_valid_from::TIMESTAMPTZ  
    FROM renamed_column
)

SELECT 
    product_model_id
    , product_model_name 
    , catalog_description 
    , dbt_valid_to
    , dbt_valid_from
FROM cast_type