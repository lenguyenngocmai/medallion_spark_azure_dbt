{{config(
      file_format = "delta",
      location_root = "/mnt/silver/customer",
)}}  

WITH renamed_column AS (
    SELECT
        CustomerId AS customer_id
        , NameStyle AS customer_name_style
        , Title AS customer_title
        , FirstName AS first_name
        , MiddleName AS middle_name
        , LastName AS last_name
        , Suffix AS suffix 
        , CompanyName AS company_name
        , SalesPerson AS sales_person
        , EmailAddress AS email_address
        , Phone AS phone
        , dbt_valid_from
        , dbt_valid_to
    FROM {{ ref('model_name') }}
)

, cast_type AS (
    SELECT
        customer_id::TEXT
        , customer_name_style::TEXT
        , customer_title::TEXT
        , first_name::TEXT
        , middle_name::TEXT
        , last_name::TEXT
        , suffix::TEXT 
        , company_name::TEXT
        , sales_person::TEXT
        , email_address::TEXT
        , phone::TEXT
        , dbt_valid_from::TIMESTAMPTZ
        , dbt_valid_to::TIMESTAMPTZ
    FROM renamed_column

)

SELECT
    customer_id
    , customer_name_style
    , customer_title
    , first_name
    , middle_name
    , last_name
    , suffix 
    , company_name
    , sales_person
    , CASE 
        WHEN sales_person IS NOT NULL THEN 'B2B'
        ELSE 'B2C'
    END AS business_group

    , email_address
    , phone
    , CASE 
        WHEN phone IS NULL AND email_address IS NULL THEN 'no digital touchpoint'
        ELSE 'has digital touchpoint'
    END AS has_digital_touchpoint
    
    , dbt_valid_from
    , dbt_valid_to 
FROM cast_type