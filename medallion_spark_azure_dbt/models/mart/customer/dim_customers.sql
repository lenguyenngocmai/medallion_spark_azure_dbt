{{
    config(
        materialized = "table",
        file_format = "delta",
        location_root = "/mnt/gold/customers"
    )
}}
---Assume that 1 customer have 1 address at the one period time
WITH stg__saleslt__customer AS (
    SELECT *
        , CONCAT(
            IFNULL(first_name, '')
            , IFNULL(middle_name, '')
            , IFNULL(last_name, '')
        ) AS full_name
    FROM {{ ref('stg__saleslt__customer') }}
    WHERE dbt_valid_to IS NULL
)

, stg__saleslt__customeraddress AS (
    SELECT *
    FROM {{ ref('stg__saleslt__customeraddress') }}
    WHERE dbt_valid_to IS NULL 
)

, stg__saleslt__address AS (
    SELECT *
    FROM {{ ref('stg__saleslt__address') }}
    WHERE dbt_valid_to IS NULL 
)

SELECT 
    customer.customer_id
    , customer.customer_title
    , customer.full_name
    , customer.suffix 
    , customer.company_name
    , customer.sales_person
    , customer.business_group
    , customer.has_digital_touchpoint
    , COALESCE(address.city, 'Undefined') AS city 
    , COALESCE(address.state_province, 'Undefined') AS state_province 
    , COALESCE(address.country, 'Undefined') AS country 
    , COALESCE(address.postal_code, 'Undefined') AS postal_code 

FROM stg__saleslt__customer AS customer
LEFT JOIN stg__saleslt__customeraddress AS customeraddress
    ON customer.customer_id = customeraddress.customer_id
LEFT JOIN stg__saleslt__address AS address
    ON customeraddress.address_id = address.address_id