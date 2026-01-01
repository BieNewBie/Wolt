{{ config(
    materialized='incremental',
    unique_key='purchase_key',
    on_schema_change='append_new_columns',
    post_hook='ANALYZE {{ this }}'
) }}

-- Fact table: Purchases
-- Grain: One row per purchase
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
        is_on_promotion,
        -- Calculate line item value (before discount)
        round((item_count * product_price_incl_vat), 2) as line_item_value_before_discount,
        -- Calculate discount amount
        case 
            when is_on_promotion then
                -- Discount = original price - discounted price with VAT
                round((item_count * product_price_incl_vat) - 
                (item_count * (product_price_excl_vat * (1 - discount_percentage / 100.0)) * (1 + vat_rate_percent / 100.0)), 2)
            else
                0.0
        end as discount_amount
    from purchases_with_promos
),

purchase_aggregates as (
    select
        purchase_key,
        customer_key,
        time_order_received_utc,
        order_date,
        delivery_distance_meters,
        wolt_service_fee,
        courier_base_fee,
        total_basket_value,
        round(sum(line_item_value_before_discount), 2) as total_basket_value_before_discounts,
        sum(item_count) as total_items,
        count(distinct item_key) as unique_items,
        sum(case when is_on_promotion then item_count else 0 end) as items_on_promotion_count,
        sum(discount_amount) as promotion_discount_amount
    from purchase_line_items
    group by 
        purchase_key,
        customer_key,
        time_order_received_utc,
        order_date,
        delivery_distance_meters,
        wolt_service_fee,
        courier_base_fee,
        total_basket_value
)

select
    -- Foreign keys (for joining to dimensions at query time)
    purchase_key,
    customer_key,
    order_date,
    
    -- Measures (only metrics/aggregatable values)
    total_basket_value,
    total_basket_value_before_discounts,
    total_items,
    unique_items,
    items_on_promotion_count,
    promotion_discount_amount,
    wolt_service_fee,
    courier_base_fee,
    round((total_basket_value + wolt_service_fee + courier_base_fee), 2) as total_order_value,
    
    -- Calculated metrics (derived measures)
    round((total_basket_value / nullif(total_items, 0)), 2) as avg_item_price,
    round((wolt_service_fee + courier_base_fee), 2) as total_fees,
    round(((wolt_service_fee + courier_base_fee) / nullif(total_basket_value, 0) * 100), 2) as fees_as_percent_of_basket,
    case 
        when items_on_promotion_count > 0 then true 
        else false 
    end as has_promotion_items,
    
    -- Degenerate dimensions (transaction-level attributes)
    delivery_distance_meters,
    time_order_received_utc,
    current_timestamp as load_time
from purchase_aggregates

