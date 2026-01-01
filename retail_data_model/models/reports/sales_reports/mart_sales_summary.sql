{{ config(
    materialized='incremental',
    unique_key=['purchase_key', 'item_key'],
    on_schema_change='append_new_columns',
    post_hook='ANALYZE {{ this }}'
) }}

-- Sales Summary Mart (Platinum Layer)
-- Grain: One row per purchase line item with all dimensions pre-joined
-- Purpose: Enable analysts to answer business questions with minimal SQL
-- Pre-joins all dimensions and includes all calculated metrics

with fact_line_items as (
    select * from {{ ref('fact_purchase_line_items') }}
    {% if is_incremental() %}
        where time_order_received_utc > (select max(time_order_received_utc) from {{ this }})
    {% endif %}
),

dim_items as (
    select * from {{ ref('dim_items') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_dates as (
    select * from {{ ref('dim_dates') }}
),

dim_promotions as (
    select * from {{ ref('dim_promotions') }}
),

-- Join fact with items dimension (SCD Type 2 join)
fact_with_items as (
    select
        f.*,
        i.item_name_en,
        i.item_category,
        i.brand_name,
        i.product_price_incl_vat,
        i.product_price_excl_vat,
        i.vat_rate_percent,
        i.currency,
        i.weight_grams,
        i.is_current as item_is_current
    from fact_line_items f
    inner join dim_items i
        on f.item_key = i.item_key
        and f.time_order_received_utc >= i.valid_from
        and f.time_order_received_utc < i.valid_to
),

-- Join with customers dimension
fact_with_customers as (
    select
        f.*,
        c.first_purchase_date,
        c.last_purchase_date,
        c.customer_since_date,
        c.days_since_first_purchase,
        c.days_since_last_purchase,
        -- Customer type flags
        case 
            when f.order_date = c.first_purchase_date then true 
            else false 
        end as is_first_purchase
    from fact_with_items f
    left join dim_customers c
        on f.customer_key = c.customer_key
),

-- Join with dates dimension
fact_with_dates as (
    select
        f.*,
        d.year,
        d.quarter,
        d.month,
        d.week,
        d.day_of_week,
        d.day_of_month,
        d.day_of_year,
        d.is_weekend,
        d.season,
        d.month_name,
        d.day_name
    from fact_with_customers f
    left join dim_dates d
        on f.order_date = d.date
),

-- Join with promotions dimension (for promo details)
fact_with_promo_details as (
    select
        f.*,
        p.promo_type,
        p.discount_percentage as promo_discount_percentage,
        p.promo_start_date,
        p.promo_end_date
    from fact_with_dates f
    left join dim_promotions p
        on f.item_key = p.item_key
        and f.order_date >= p.promo_start_date
        and f.order_date <= p.promo_end_date_exclusive
)

select
    -- Primary keys and identifiers
    purchase_key,
    customer_key,
    item_key,
    order_date,
    
    -- Date attributes (from dim_dates)
    year,
    quarter,
    month,
    week,
    day_of_week,
    day_of_month,
    day_of_year,
    is_weekend,
    season,
    month_name,
    day_name,
    
    -- Item attributes (from dim_items)
    item_name_en,
    item_category,
    brand_name,
    product_price_incl_vat,
    product_price_excl_vat,
    vat_rate_percent,
    currency,
    weight_grams,
    item_is_current,
    
    -- Customer attributes (from dim_customers)
    first_purchase_date,
    last_purchase_date,
    customer_since_date,
    days_since_first_purchase,
    days_since_last_purchase,
    is_first_purchase,
    
    -- Promotion attributes (from dim_promotions)
    is_on_promotion,
    promo_type,
    promo_discount_percentage,
    promo_start_date,
    promo_end_date,
    
    -- Measures (from fact_purchase_line_items)
    item_count,
    unit_price_at_purchase,
    line_item_value_before_discount,
    line_item_value_after_discount,
    discount_amount,
    
    -- Degenerate dimensions (from fact_purchase_line_items)
    delivery_distance_meters,
    {{ delivery_distance_category('delivery_distance_meters') }} as delivery_distance_category,
    wolt_service_fee,
    courier_base_fee,
    total_basket_value,
    time_order_received_utc,
    
    -- Calculated metrics for easy analysis
    round((wolt_service_fee + courier_base_fee), 2) as total_fees,
    round((wolt_service_fee + courier_base_fee) / nullif(total_basket_value, 0) * 100, 2) as fees_as_percent_of_basket,
    
    current_timestamp as load_time
from fact_with_promo_details

