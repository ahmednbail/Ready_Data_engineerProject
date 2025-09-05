{{config(
    materialized='incremental',
    alias='dim_sellers_Ahmed',
    schema='analytics',
    unique_key='seller_id',
    incremental_strategy='merge'
)}}

SELECT  
    seller_id,
    seller_zip_code_prefix, 
    seller_city,
    seller_state,
FROM `ready-de26.project_landing.sellers_ahmednabil`