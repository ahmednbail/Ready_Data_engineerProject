{{config(
    materialized='incremental',
    alias='dim_payment_Ahmed',
    schema='analytics',
    unique_key='payment_id', 
    incremental_strategy='merge'
)}}

SELECT
      {{ dbt_utils.generate_surrogate_key([ 'order_id',
        'payment_sequential',
        'payment_type',
        'payment_installments',
        'payment_value']) }} as payment_id,
         order_id,
         payment_type,
         payment_installments,
         payment_value,
FROM `ready-de26.project_landing.order_payments_ahmednbail`