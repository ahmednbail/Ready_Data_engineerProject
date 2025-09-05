{{ config(
    materialized='incremental',
    alias='FactOrders_Ahmed',
    schema='analytics',
    incremental_strategy='merge'
) }}

SELECT  
o.order_id,
c.customer_id, 
p.product_id,
s.seller_id,
d.date_id,
py.payment_id,
py.payment_value,
oi.orders_items_id,
oi.quantity, 
oi.price,
oi.freight_value
FROM `ready-de26.project_landing.Ahmednabil_orders` o
LEFT JOIN {{ ref('DimCustomer_Ahmed') }} c
    on o.customer_id = c.customer_id
LEFT JOIN {{ ref('DimDate_Ahmed') }} d
    on CAST(FORMAT_DATE('%Y%m%d', DATE(o.order_purchase_timestamp)) AS INT64) = d.date_id 
LEFT JOIN {{ ref('Dimorder_item_Ahmed') }} oi
    on o.order_id = oi.order_id
LEFT JOIN {{ ref('DimPayment_Ahmed') }} py
    on o.order_id = py.order_id
LEFT JOIN `ready-de26.project_landing.Ahmednabil_products` p
    ON oi.product_id = p.product_id
LEFT JOIN `ready-de26.project_landing.sellers_ahmednabil` s
    on oi.seller_id = s.seller_id