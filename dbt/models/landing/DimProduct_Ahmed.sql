{{config(
    materialized='incremental',
    alias='dim_products_Ahmed',
    schema='analytics',
    unique_key='product_id',
    incremental_strategy='merge'
)}}

SELECT  DISTINCT
     product_id,
     product_width_cm,
     product_weight_g,
     product_length_cm,
FROM `ready-de26.project_landing.Ahmednabil_products`