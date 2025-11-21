{% snapshot case_check_hard_deletes %}

{{
    config(
      unique_key='a',
      strategy='check',
      check_cols=["a"],
      target_schema='dry_run',
      hard_deletes='new_record'
    )
}}

select a,
       b,
       c
from {{ ref("first_layer") }}

{% endsnapshot %}