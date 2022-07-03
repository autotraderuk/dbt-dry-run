{% snapshot case_invalid_unique_key_snapshot %}

{{
    config(
      unique_key='wrong_column',
      strategy='check',
      check_cols='all',
      target_schema='dry_run'
    )
}}

select a,
       b,
       c
from {{ ref("first_layer") }}

{% endsnapshot %}