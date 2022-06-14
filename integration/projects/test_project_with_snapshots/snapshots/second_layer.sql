{% snapshot second_layer_snapshot %}

{{
    config(
      unique_key='a',
      strategy='timestamp',
      updated_at='updated_at',
      target_schema='dry_run'
    )
}}

select *,
       DATE("2021-01-01") as updated_at
from {{ ref("first_layer") }}

{% endsnapshot %}