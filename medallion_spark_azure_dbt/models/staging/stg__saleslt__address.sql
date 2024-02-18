{{config(
      file_format = "delta",
      location_root = "/mnt/silver/address",
)}}  

WITH renamed_column AS (
    SELECT
        AddressID AS address_id
        , AddressLine1 AS address_line_1
        , AddressLine2 AS address_line_2
        , City AS city
        , StateProvince AS state_province
        , CountryRegion AS country
        , PostalCode AS postal_code
        , dbt_valid_from 
        , dbt_valid_to
    FROM {{ ref('address_snapshot') }}
)

, cast_type AS (
    SELECT
        address_id::TEXT
        , address_line_1::TEXT
        , address_line_2::TEXT
        , city::TEXT
        , state_province::TEXT
        , country::TEXT
        , postal_code::TEXT
        , dbt_valid_from::TIMESTAMPTZ
        , dbt_valid_to::TIMESTAMPTZ
    FROM renamed_column
)

SELECT 
    address_id
    , address_line_1
    , address_line_2
    , city
    , state_province
    , country
    , postal_code
    , dbt_valid_from
    , dbt_valid_to
FROM cast_type