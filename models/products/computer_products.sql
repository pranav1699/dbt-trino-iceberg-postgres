


{{
    config(
      materialized = 'incremental',
      unique_key='id',
      incremental_strategy='merge'
      )
}}


select * from {{ var("postgres_catalog") }}.{{ var("postgres_schema") }}.computer_products
{% if is_incremental() %}

  where time_added > (select max(time_added) from {{ this }})

{% endif %}