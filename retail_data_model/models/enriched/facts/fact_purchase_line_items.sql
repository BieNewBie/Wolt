{{ config(
    materialized='incremental',
    unique_key=['purchase_key', 'item_key'],
    on_schema_change='append_new_columns',
    post_hook='ANALYZE {{ this }}'
) }}

-- Fact table: Purchase line items
-- Grain: One row per item in each purchase
-- Pure dimensional modeling: Only foreign keys, measures, and degenerate dimensions

with purchases as (
    select * from {{ ref('stg_purchase_logs') }}
    {% if is_incremental() %}
        where time_order_received_utc > (select max(time_order_received_utc) from {{ this }})
    {% endif %}
),

items_scd as (
    select * from {{ ref('dim_items') }}
),

promos_active as (
    select * from {{ ref('dim_promotions') }}
),

purchases_with_items as (
    select
        p.*,
        i.product_price_incl_vat,
        i.product_price_excl_vat,
        i.vat_rate_percent
    from purchases p
    inner join items_scd i
        on p.item_key = i.item_key
        and p.time_order_received_utc >= i.valid_from
        and p.time_order_received_utc < i.valid_to
),

purchases_with_promos as (
    select
        p.*,
        pr.discount_percentage,
        case 
            when pr.item_key is not null then true 
            else false 
        end as is_on_promotion
    from purchases_with_items p
    left join promos_active pr
        on p.item_key = pr.item_key
        and p.order_date >= pr.promo_start_date
        and p.order_date <= pr.promo_end_date_exclusive
),

purchase_line_items as (
    select
        purchase_key,
        customer_key,
        time_order_received_utc,
        order_date,
        delivery_distance_meters,
        wolt_service_fee,
        courier_base_fee,
        total_basket_value,
        item_key,
        item_count,
        coalesce(discount_percentage, 0) as discount_percentage,
        is_on_promotion,
        -- Calculate unit price at purchase following proper accounting:
        -- If on promotion: apply discount to net price, then recalculate VAT
        -- If not on promotion: use price including VAT directly
        case 
            when is_on_promotion then
                -- Discount applied to net price, then VAT recalculated
                round((product_price_excl_vat * (1 - discount_percentage / 100.0)) * (1 + vat_rate_percent / 100.0), 2)
            else
                -- No discount: use price including VAT
                product_price_incl_vat
        end as unit_price_at_purchase,
        -- Calculate line item value (before discount)
        round((item_count * product_price_incl_vat), 2) as line_item_value_before_discount,
        -- Calculate line item value (after discount)
        case 
            when is_on_promotion then
                -- Discount on net price, then VAT recalculated
                round((item_count * (product_price_excl_vat * (1 - discount_percentage / 100.0)) * (1 + vat_rate_percent / 100.0)), 2)
            else
                -- No discount: use price including VAT
                round((item_count * product_price_incl_vat), 2)
        end as line_item_value_after_discount,
        -- Calculate discount amount
        case 
            when is_on_promotion then
                -- Discount = original price - discounted price with VAT
                round((item_count * product_price_incl_vat) - 
                (item_count * (product_price_excl_vat * (1 - discount_percentage / 100.0)) * (1 + vat_rate_percent / 100.0)), 2)
            else
                0.0
        end as discount_amount,
        current_timestamp as load_time
    from purchases_with_promos
)

select
    -- Foreign keys (for joining to dimensions at query time)
    purchase_key,
    customer_key,
    item_key,
    order_date,
    
    -- Measures (only metrics/aggregatable values)
    item_count,
    unit_price_at_purchase,
    line_item_value_before_discount,
    line_item_value_after_discount,
    discount_amount,
    is_on_promotion,
    
    -- Degenerate dimensions (transaction-level attributes that don't warrant separate dimensions)
    -- These are part of the transaction itself, not descriptive attributes from dimensions
    delivery_distance_meters,
    wolt_service_fee,
    courier_base_fee,
    total_basket_value,
    time_order_received_utc,
    
    load_time
from purchase_line_items

