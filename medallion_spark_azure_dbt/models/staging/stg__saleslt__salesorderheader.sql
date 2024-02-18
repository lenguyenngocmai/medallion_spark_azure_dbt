{{config(
      file_format = "delta",
      location_root = "/mnt/silver/salesorderheader",
)}}  


WITH renamed_column AS (
      SELECT
            SalesOrderID AS sales_order_id
            , RevisionNumber AS revision_number
            , OrderDate AS order_date
            , DueDate AS due_date
            , ShipDate AS ship_date
            , Status AS order_status
            , OnlineOrderFlag AS is_online_order
            , SalesOrderNumber AS sales_order_number
            , PurchaseOrderNumber AS purchase_order_number
            , AccountNumber AS account_number
            , CustomerID AS customer_id
            , ShipToAddressID AS ship_to_address_id
            , BillToAddressID AS bill_to_address_id
            , ShipMethod AS ship_method
            , CreditCardApprovalCode AS credit_card_approval_code
            , SubTotal AS subtotal_amount
            , TaxAmt AS  tax_amount
            , Freight AS freight
            , TotalDue AS total_due_amount
            , Comment AS comment 
            , dbt_valid_from
            , dbt_valid_to
      FROM {{ ref('salesorderheader_snapshot') }}
      
)

, cast_type AS (
      SELECT
           sales_order_id::TEXT 
            , revision_number::TEXT
            , order_date::DATE
            , due_date::DATE
            , ship_date::DATE
            , order_status::TEXT  
            , is_online_order::TEXT
            , sales_order_number::TEXT
            , purchase_order_number::TEXT
            , account_number::TEXT
            , customer_id::TEXT
            , ship_to_address_id::TEXT
            , bill_to_address_id::TEXT
            , ship_method::TEXT
            , credit_card_approval_code::TEXT
            , subtotal_amount::NUMERIC
            ,  tax_amount::NUMERIC
            , freight::TEXT
            , total_due_amount::NUMERIC
            , comment::TEXT
            , dbt_valid_from::TIMESTAMPTZ
            , dbt_valid_to::TIMESTAMPTZ
      FROM renamed_column
)

SELECT 
      sales_order_id
       , revision_number
       , order_date
       , due_date
       , ship_date
       , order_status  
       , is_online_order
       , sales_order_number
       , purchase_order_number
       , account_number
       , customer_id
       , ship_to_address_id
       , bill_to_address_id
       , ship_method
       , credit_card_approval_code
       , subtotal_amount
       ,  tax_amount
       , freight
       , total_due_amount
       , comment
       , dbt_valid_from
       , dbt_valid_to
FROM cast_type