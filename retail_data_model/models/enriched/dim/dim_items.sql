{{ config(
    materialized='table',
    post_hook='ANALYZE {{ this }}'
) }}

-- SCD Type 2 table for items
-- Creates valid_from and valid_to dates for historical tracking

with item_logs as (
    select * from {{ ref('stg_item_logs') }}
),

ordered_items as (
    select
        *,
        lead(time_log_created_utc) over (
            partition by item_key 
            order by time_log_created_utc
        ) as next_time_log_created_utc
    from item_logs
)

select
    log_item_id,
    item_key,
    time_log_created_utc as valid_from,
    coalesce(
        next_time_log_created_utc,
        '9999-12-31'::timestamp
    ) as valid_to,
    case 
        when next_time_log_created_utc is null then true 
        else false 
    end as is_current,
    brand_name,
    item_category,
    item_name_en,
    product_price_incl_vat,
    product_price_excl_vat,
    vat_rate_percent,
    currency,
    weight_grams,
    number_of_units,
    time_item_created_source_utc,
    current_timestamp as load_time
from ordered_items

