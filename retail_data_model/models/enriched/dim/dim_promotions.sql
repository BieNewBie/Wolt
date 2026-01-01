{{ config(
    materialized='table',
    post_hook='ANALYZE {{ this }}'
) }}

-- Active promos table for date range joining
-- Handles promo end date logic (exclusive at midnight)

with promos as (
    select * from {{ ref('stg_promos') }}
)

select
    promo_start_date,
    promo_end_date,
    -- Promo end date is exclusive, so valid until end of previous day
    (promo_end_date - interval '1 day')::date as promo_end_date_exclusive,
    item_key,
    promo_type,
    discount_percentage,
    current_timestamp as load_time
from promos

