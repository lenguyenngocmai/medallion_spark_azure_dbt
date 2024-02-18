{% snapshot customeraddress_snapshot %}

{{
    config(
      file_format = "delta",
      location_root = "/mnt/silver/customeraddress",

      target_schema='snapshots',
      invalidate_hard_deletes=True,
      unique_key="CustomerId||'-'||AddressId",
      strategy='check',
      check_cols='all'
    )
}}

with customeraddress_snapshot as (
    select
        CustomerId,
        AddressId,
        AddressType
    from {{ source('bronze', 'customeraddress') }}
)
select *
from customeraddress_snapshot

{% endsnapshot %}