{{config(
    materialized='incremental',
    alias='dim_customers_Ahmed',
    schema='analytics',
    unique_key='customer_id',
    incremental_strategy='merge'
)}}

SELECT  DISTINCT
   customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
FROM `ready-de26.project_landing.Ahmednabil_customers`
