{{config(
      file_format = "delta",
      location_root = "/mnt/silver/customeraddress",
)}}  

WITH renamed_column AS (
      SELECT 
            CustomerId AS customer_id
            , AddressId AS address_id
            , AddressType AS address_type
            , dbt_valid_to
            , dbt_valid_from
      FROM {{ ref('customeraddress_snapshot') }}
)

, cast_type AS (
      SELECT 
            customer_id::TEXT
            , address_id::TEXT
            , address_type::TEXT
            , dbt_valid_to::TIMESTAMPTZ
            , dbt_valid_from::TIMESTAMPTZ
      FROM renamed_column
)

SELECT
      customer_id
      , address_id
      , address_type
      , dbt_valid_to
      , dbt_valid_from 
FROM cast_type