{{ config(
    materialized='incremental',
    alias='dim_geolocation_Ahmed',
    schema='analytics',
    unique_key='geolocation_id', 
    incremental_strategy='merge'
) }}

select DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'geolocation_zip_code_prefix',
        'geolocation_city',
        'geolocation_state'
    ]) }} as geolocation_id,   -- primary key

    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state

FROM `ready-de26.project_landing.Ahmednabil_geolocation`
