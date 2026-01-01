{{ config(
    materialized='table'
) }}

-- Standardized promos table
-- This model cleans and standardizes the raw promo data

with source_data as (
    select * from {{ source('landing', 'snack_store_promos') }}
),

standardized as (
    select
        PROMO_START_DATE::date as promo_start_date,
        PROMO_END_DATE::date as promo_end_date,
        ITEM_KEY as item_key,
        PROMO_TYPE as promo_type,
        DISCOUNT_IN_PERCENTAGE::int as discount_percentage,
        current_timestamp as load_time
    from source_data
)

select * from standardized

