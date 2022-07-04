{% snapshot case_check_single_column_snapshot %}

{{
    config(
      unique_key='a',
      strategy='check',
      check_cols=["a"],
      target_schema='dry_run'
    )
}}

select a,
       b,
       c
from {{ ref("first_layer") }}

{% endsnapshot %}