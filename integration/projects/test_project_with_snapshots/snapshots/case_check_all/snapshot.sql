{% snapshot case_check_all_snapshot %}

{{
    config(
      unique_key='a',
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