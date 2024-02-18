{% snapshot customer_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/customer",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key='CustomerId',
      strategy='check',
      check_cols='all'
    )
}}

with customer_snapshot as (
    select
        CustomerId,
        NameStyle,
        Title,
        FirstName,
        MiddleName,
        LastName,
        Suffix,
        CompanyName,
        SalesPerson,
        EmailAddress,
        Phone
    from {{ source('bronze', 'customer') }}
)
select *
from customer_snapshot

{% endsnapshot %}