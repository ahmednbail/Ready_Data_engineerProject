{{config(
    materialized='incremental',
    alias='dim_order_item_Ahmed',
    schema='analytics',
    incremental_strategy='merge'
)}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id', 'seller_id', 'product_id']) }} AS orders_items_id,
    order_id,
    seller_id,
    product_id,
    COUNT(order_item_id) AS quantity,
    price,
    freight_value
FROM `ready-de26.project_landing.Ahmednabil_Order_items`
GROUP BY 
    order_id, 
    seller_id, 
    product_id, 
    price, 
    freight_value
