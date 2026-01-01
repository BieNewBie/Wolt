{{ config(
    materialized='table',
    post_hook='ANALYZE {{ this }}'
) }}

-- Customer Monthly Summary Mart (Platinum Layer)
-- Grain: One row per customer per month with monthly metrics, running totals, and segments
-- Purpose: Enable analysts to answer customer behavior over time and retention questions with minimal SQL
-- Supports: Retention analysis, first-time customer analysis, engagement trends, customer segmentation
-- Base: Built from mart_customer_monthly_metrics with running totals and segments added

with monthly_metrics as (
    select * from {{ ref('mart_customer_monthly_metrics') }}
),

-- Calculate running totals using window functions
monthly_with_totals as (
    select
        *,
        -- Running totals (till-date)
        sum(purchases_this_month) over (
            partition by customer_key 
            order by year, month 
            rows between unbounded preceding and current row
        ) as total_purchases_till_date,
        round(sum(revenue_this_month) over (
            partition by customer_key 
            order by year, month 
            rows between unbounded preceding and current row
        ), 2) as total_revenue_till_date,
        round(sum(basket_value_this_month) over (
            partition by customer_key 
            order by year, month 
            rows between unbounded preceding and current row
        ), 2) as total_basket_value_till_date,
        sum(items_purchased_this_month) over (
            partition by customer_key 
            order by year, month 
            rows between unbounded preceding and current row
        ) as total_items_till_date,
        sum(items_on_promotion_this_month) over (
            partition by customer_key 
            order by year, month 
            rows between unbounded preceding and current row
        ) as total_items_on_promotion_till_date,
        round(sum(promotion_discount_this_month) over (
            partition by customer_key 
            order by year, month 
            rows between unbounded preceding and current row
        ), 2) as total_promotion_discount_till_date,
        sum(purchases_with_promotions_this_month) over (
            partition by customer_key 
            order by year, month 
            rows between unbounded preceding and current row
        ) as total_purchases_with_promotions_till_date,
        round(sum(wolt_service_fees_this_month) over (
            partition by customer_key 
            order by year, month 
            rows between unbounded preceding and current row
        ), 2) as total_wolt_service_fees_till_date,
        round(sum(courier_fees_this_month) over (
            partition by customer_key 
            order by year, month 
            rows between unbounded preceding and current row
        ), 2) as total_courier_fees_till_date
    from monthly_metrics
),

-- Add calculated metrics and segments
final as (
    select
        -- Base columns from monthly_metrics
        customer_key,
        year,
        month,
        quarter,
        month_start_date,
        month_end_date,
        -- Cohort information
        cohort_year,
        cohort_month,
        cohort_month_label,
        first_purchase_date,
        months_since_first_purchase,
        -- Monthly activity
        purchases_this_month,
        revenue_this_month,
        basket_value_this_month,
        items_purchased_this_month,
        items_on_promotion_this_month,
        promotion_discount_this_month,
        purchases_with_promotions_this_month,
        wolt_service_fees_this_month,
        courier_fees_this_month,
        avg_order_value_this_month,
        avg_basket_value_this_month,
        -- Flags
        is_first_month,
        -- First month promotion details
        all_items_on_promotion_first_month,
        has_promotion_items_first_month,
        promotion_only_purchase_first_month,
        promotion_usage_percentage_first_month,
        -- Running totals (till-date)
        total_purchases_till_date,
        total_revenue_till_date,
        total_basket_value_till_date,
        total_items_till_date,
        total_items_on_promotion_till_date,
        total_promotion_discount_till_date,
        total_purchases_with_promotions_till_date,
        total_wolt_service_fees_till_date,
        total_courier_fees_till_date,
        -- Calculated metrics from running totals
        round((total_purchases_with_promotions_till_date * 100.0 / nullif(total_purchases_till_date, 0)), 2) as promotion_usage_percentage_till_date,
        round((total_items_on_promotion_till_date * 100.0 / nullif(total_items_till_date, 0)), 2) as items_on_promotion_percentage_till_date,
        round((total_promotion_discount_till_date * 100.0 / nullif(total_basket_value_till_date, 0)), 2) as discount_savings_percentage_till_date,
        -- Customer segment at month end (based on total purchases till date) - using quartile-based thresholds
        -- Thresholds: 1, 2-25 (P25), 26-40 (P50), 41-63 (P75), 64+ (above P75)
        case
            when total_purchases_till_date = 0 then 'No Purchases'
            when total_purchases_till_date = 1 then 'One-Time Customer'
            when total_purchases_till_date between 2 and 25 then 'Occasional Customer'  -- Up to 25th percentile
            when total_purchases_till_date between 26 and 40 then 'Regular Customer'    -- 25th-50th percentile
            when total_purchases_till_date between 41 and 63 then 'Frequent Customer'    -- 50th-75th percentile
            else 'Very Frequent Customer'  -- Above 75th percentile
        end as customer_segment_at_end_of_month,
        current_timestamp as load_time
    from monthly_with_totals
)

select * from final
order by customer_key, year, month
