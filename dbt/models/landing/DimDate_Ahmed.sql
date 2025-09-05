
{{ config(
    materialized='incremental',
    alias='dim_date_Ahmed',
    schema='analytics',
    unique_key='date_id', 
    incremental_strategy='merge'
) }}


select distinct
    cast(format_date('%Y%m%d', date(order_purchase_timestamp)) as int64) as date_id,
    date(order_purchase_timestamp) as full_date,
    extract(day from date(order_purchase_timestamp)) as day,
    extract(month from date(order_purchase_timestamp)) as month,
    extract(quarter from date(order_purchase_timestamp)) as quarter,
    extract(year from date(order_purchase_timestamp)) as year
from `ready-de26.project_landing.Ahmednabil_orders`
where order_purchase_timestamp is not null
order by full_date







