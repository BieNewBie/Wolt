{{ config(
    materialized='incremental',
    unique_key='customer_key',
    on_schema_change='append_new_columns',
    post_hook='ANALYZE {{ this }}'
) }}

-- Customer Summary Mart (Platinum Layer)
-- Grain: One row per customer with aggregated metrics
-- Purpose: Enable analysts to answer customer behavior questions with minimal SQL
-- Pre-aggregates customer-level metrics from fact tables

with fact_purchases as (
    select * from {{ ref('fact_purchases') }}
    {% if is_incremental() %}
        where order_date > (select max(last_purchase_date) from {{ this }})
    {% endif %}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

-- Aggregate purchase-level metrics from new purchases (incremental) or all purchases (full refresh)
purchase_metrics_new as (
    select
        customer_key,
        count(distinct purchase_key) as total_purchases,
        round(sum(total_order_value), 2) as total_revenue,
        round(sum(total_basket_value), 2) as total_basket_value,
        round(sum(wolt_service_fee), 2) as total_wolt_service_fees,
        round(sum(courier_base_fee), 2) as total_courier_fees,
        round(sum(total_fees), 2) as total_fees,
        sum(total_items) as total_items_purchased,
        sum(items_on_promotion_count) as total_items_on_promotion,
        round(sum(promotion_discount_amount), 2) as total_promotion_discounts,
        count(distinct case when has_promotion_items then purchase_key end) as purchases_with_promotions
    from fact_purchases
    group by customer_key
),

{% if is_incremental() %}
-- For incremental: merge new metrics with existing customer records
existing_customers as (
    select * from {{ this }}
),

purchase_metrics as (
    select
        coalesce(e.customer_key, n.customer_key) as customer_key,
        coalesce(e.total_purchases, 0) + coalesce(n.total_purchases, 0) as total_purchases,
        round(coalesce(e.total_revenue, 0) + coalesce(n.total_revenue, 0), 2) as total_revenue,
        round(coalesce(e.total_basket_value, 0) + coalesce(n.total_basket_value, 0), 2) as total_basket_value,
        round(coalesce(e.total_wolt_service_fees, 0) + coalesce(n.total_wolt_service_fees, 0), 2) as total_wolt_service_fees,
        round(coalesce(e.total_courier_fees, 0) + coalesce(n.total_courier_fees, 0), 2) as total_courier_fees,
        round(coalesce(e.total_fees, 0) + coalesce(n.total_fees, 0), 2) as total_fees,
        coalesce(e.total_items_purchased, 0) + coalesce(n.total_items_purchased, 0) as total_items_purchased,
        coalesce(e.total_items_on_promotion, 0) + coalesce(n.total_items_on_promotion, 0) as total_items_on_promotion,
        round(coalesce(e.total_promotion_discounts, 0) + coalesce(n.total_promotion_discounts, 0), 2) as total_promotion_discounts,
        coalesce(e.purchases_with_promotions, 0) + coalesce(n.purchases_with_promotions, 0) as purchases_with_promotions,
        -- Recalculate averages from totals
        round((coalesce(e.total_revenue, 0) + coalesce(n.total_revenue, 0)) / nullif(coalesce(e.total_purchases, 0) + coalesce(n.total_purchases, 0), 0), 2) as avg_order_value,
        round((coalesce(e.total_basket_value, 0) + coalesce(n.total_basket_value, 0)) / nullif(coalesce(e.total_purchases, 0) + coalesce(n.total_purchases, 0), 0), 2) as avg_basket_value,
        round((coalesce(e.total_items_purchased, 0) + coalesce(n.total_items_purchased, 0)) / nullif(coalesce(e.total_purchases, 0) + coalesce(n.total_purchases, 0), 0), 2) as avg_items_per_order,
        -- For fees_as_percent_of_basket, we need to recalculate from the merged totals
        round(((coalesce(e.total_fees, 0) + coalesce(n.total_fees, 0)) / nullif(coalesce(e.total_basket_value, 0) + coalesce(n.total_basket_value, 0), 0) * 100), 2) as avg_fees_as_percent_of_basket
    from existing_customers e
    full outer join purchase_metrics_new n
        on e.customer_key = n.customer_key
),
{% else %}
-- For full refresh: use new metrics directly and calculate averages
purchase_metrics as (
    select
        customer_key,
        total_purchases,
        total_revenue,
        total_basket_value,
        total_wolt_service_fees,
        total_courier_fees,
        total_fees,
        total_items_purchased,
        total_items_on_promotion,
        total_promotion_discounts,
        purchases_with_promotions,
        round(total_revenue / nullif(total_purchases, 0), 2) as avg_order_value,
        round(total_basket_value / nullif(total_purchases, 0), 2) as avg_basket_value,
        round(total_items_purchased / nullif(total_purchases, 0), 2) as avg_items_per_order,
        round((total_fees / nullif(total_basket_value, 0) * 100), 2) as avg_fees_as_percent_of_basket
    from purchase_metrics_new
    -- Note: total_revenue, total_basket_value, total_wolt_service_fees, total_courier_fees, 
    -- total_fees, and total_promotion_discounts are already rounded in purchase_metrics_new
),
{% endif %}

-- Join with customer dimension for descriptive attributes
customer_summary as (
    select
        c.customer_key,
        c.days_since_first_purchase,
        c.days_since_last_purchase,
        
        -- Purchase metrics
        coalesce(p.total_purchases, 0) as total_purchases,
        c.first_purchase_date as first_purchase_date,
        c.last_purchase_date as last_purchase_date,
        coalesce(p.total_revenue, 0) as total_revenue,
        coalesce(p.total_basket_value, 0) as total_basket_value,
        coalesce(p.total_wolt_service_fees, 0) as total_wolt_service_fees,
        coalesce(p.total_courier_fees, 0) as total_courier_fees,
        coalesce(p.total_fees, 0) as total_fees,
        coalesce(p.total_items_purchased, 0) as total_items_purchased,
        coalesce(p.total_items_on_promotion, 0) as total_items_on_promotion,
        coalesce(p.total_promotion_discounts, 0) as total_promotion_discounts,
        coalesce(p.purchases_with_promotions, 0) as purchases_with_promotions,
        coalesce(p.avg_order_value, 0) as avg_order_value,
        coalesce(p.avg_basket_value, 0) as avg_basket_value,
        coalesce(p.avg_items_per_order, 0) as avg_items_per_order,
        coalesce(p.avg_fees_as_percent_of_basket, 0) as avg_fees_as_percent_of_basket,
        
        -- Calculated customer behavior metrics
        datediff('day', c.first_purchase_date, c.last_purchase_date) as customer_lifetime_days,
        round((coalesce(p.purchases_with_promotions, 0) / nullif(coalesce(p.total_purchases, 0), 0) * 100), 2) as promotion_usage_percentage,
        round((coalesce(p.total_items_on_promotion, 0) / nullif(coalesce(p.total_items_purchased, 0), 0) * 100), 2) as items_on_promotion_percentage,
        round((coalesce(p.total_promotion_discounts, 0) / nullif(coalesce(p.total_basket_value, 0), 0) * 100), 2) as discount_savings_percentage,
        -- Customer segment based on quartile-based thresholds (percentile-based)
        -- Thresholds: 1, 2-25 (P25), 26-40 (P50), 41-63 (P75), 64+ (above P75)
        case 
            when coalesce(p.total_purchases, 0) = 0 then 'No Purchases'
            when coalesce(p.total_purchases, 0) = 1 then 'One-Time Customer'
            when coalesce(p.total_purchases, 0) between 2 and 25 then 'Occasional Customer'  -- Up to 25th percentile
            when coalesce(p.total_purchases, 0) between 26 and 40 then 'Regular Customer'    -- 25th-50th percentile
            when coalesce(p.total_purchases, 0) between 41 and 63 then 'Frequent Customer'    -- 50th-75th percentile
            else 'Very Frequent Customer'  -- Above 75th percentile
        end as customer_segment,
        -- Customer value segment based on quartile-based thresholds (percentile-based)
        -- Thresholds: 0, 0.01-190 (P25), 191-310 (P50), 311-490 (P75), 491+ (above P75)
        case
            when coalesce(p.total_revenue, 0) = 0 then 'No Revenue'
            when coalesce(p.total_revenue, 0) > 0 and coalesce(p.total_revenue, 0) <= 190 then 'Low Value'        -- Up to ~25th percentile
            when coalesce(p.total_revenue, 0) > 190 and coalesce(p.total_revenue, 0) <= 310 then 'Medium Value'  -- ~25th-50th percentile
            when coalesce(p.total_revenue, 0) > 310 and coalesce(p.total_revenue, 0) <= 490 then 'High Value'    -- ~50th-75th percentile
            else 'Very High Value'  -- Above ~75th percentile
        end as customer_value_segment
        
    from dim_customers c
    left join purchase_metrics p
        on c.customer_key = p.customer_key
)

select
    customer_key,
    
    -- Customer descriptive attributes
    days_since_first_purchase,
    days_since_last_purchase,
    
    -- Purchase metrics
    total_purchases,
    first_purchase_date,
    last_purchase_date,
    customer_lifetime_days,
    customer_segment,
    customer_value_segment,
    
    -- Revenue metrics
    total_revenue,
    total_basket_value,
    total_wolt_service_fees,
    total_courier_fees,
    total_fees,
    avg_order_value,
    avg_basket_value,
    
    -- Item metrics
    total_items_purchased,
    avg_items_per_order,
    
    -- Promotion metrics
    total_items_on_promotion,
    total_promotion_discounts,
    purchases_with_promotions,
    promotion_usage_percentage,
    items_on_promotion_percentage,
    discount_savings_percentage,
    
    -- Fee metrics
    avg_fees_as_percent_of_basket,
    
    -- Load time
    current_timestamp as load_time
    
from customer_summary
order by total_revenue desc

