{{ config(
    materialized='incremental',
    unique_key=['customer_key', 'year', 'month'],
    on_schema_change='append_new_columns',
    post_hook='ANALYZE {{ this }}'
) }}

-- Customer Monthly Metrics (Base Table)
-- Grain: One row per customer per month with monthly aggregated metrics only
-- Purpose: Base table containing only monthly metrics (no running totals)
-- This table is used by mart_customer_monthly_summary to calculate running totals and segments

with fact_purchases as (
    select * from {{ ref('fact_purchases') }}
    {% if is_incremental() %}
        -- Only process months from the latest month_start_date onwards (handles partial months)
        where order_date >= (
            select max(month_start_date)
            from {{ this }}
        )
    {% endif %}
),

fact_line_items as (
    select * from {{ ref('fact_purchase_line_items') }}
    {% if is_incremental() %}
        -- Only process months from the latest month_start_date onwards (handles partial months)
        where order_date >= (
            select max(month_start_date)
            from {{ this }}
        )
    {% endif %}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_dates as (
    select * from {{ ref('dim_dates') }}
),

-- Get month date ranges
month_ranges as (
    select distinct
        year,
        month,
        min(date) as month_start_date,
        max(date) as month_end_date
    from dim_dates
    group by year, month
),

-- Get customer first purchase month for cohort analysis
customer_cohorts as (
    select
        c.customer_key,
        c.first_purchase_date,
        -- Get year and month of first purchase
        extract(year from c.first_purchase_date) as cohort_year,
        extract(month from c.first_purchase_date) as cohort_month,
        -- Format as YYYY-MM for cohort identification
        strftime(c.first_purchase_date, '%Y-%m') as cohort_month_label
    from dim_customers c
),

-- Aggregate monthly purchase metrics (with month date ranges)
monthly_purchase_metrics as (
    select
        p.customer_key,
        d.year,
        d.month,
        d.quarter,
        mr.month_start_date,
        mr.month_end_date,
        count(distinct p.purchase_key) as purchases_this_month,
        round(sum(p.total_order_value), 2) as revenue_this_month,
        round(sum(p.total_basket_value), 2) as basket_value_this_month,
        sum(p.total_items) as items_purchased_this_month,
        sum(p.items_on_promotion_count) as items_on_promotion_this_month,
        round(sum(p.promotion_discount_amount), 2) as promotion_discount_this_month,
        count(distinct case when p.has_promotion_items then p.purchase_key end) as purchases_with_promotions_this_month,
        round(sum(p.wolt_service_fee), 2) as wolt_service_fees_this_month,
        round(sum(p.courier_base_fee), 2) as courier_fees_this_month,
        round(avg(p.total_order_value), 2) as avg_order_value_this_month,
        round(avg(p.total_basket_value), 2) as avg_basket_value_this_month
    from fact_purchases p
    inner join dim_dates d
        on p.order_date = d.date
    inner join month_ranges mr
        on d.year = mr.year
        and d.month = mr.month
    group by
        p.customer_key,
        d.year,
        d.month,
        d.quarter,
        mr.month_start_date,
        mr.month_end_date
),

-- Join with customer cohorts
monthly_with_cohorts as (
    select
        m.*,
        c.first_purchase_date,
        c.cohort_year,
        c.cohort_month,
        c.cohort_month_label,
        -- Calculate months since first purchase
        datediff('month', c.first_purchase_date, m.month_start_date) as months_since_first_purchase,
        -- Flags
        (m.year = c.cohort_year and m.month = c.cohort_month) as is_first_month
    from monthly_purchase_metrics m
    inner join customer_cohorts c
        on m.customer_key = c.customer_key
    where m.month_end_date >= c.first_purchase_date -- Only include the month containing first purchase and all subsequent months
),

-- Get first month promotion details for first-time customer analysis
-- Only calculate for new customers in incremental mode
first_month_promotion_details as (
    select
        li.customer_key,
        -- Check if all items in first month were on promotion
        sum(case when li.is_on_promotion then 0 else 1 end) = 0 as all_items_on_promotion_first_month,
        -- Check if any items were on promotion
        sum(case when li.is_on_promotion then 1 else 0 end) > 0 as has_promotion_items_first_month,
        -- Check if only promotion items (all items on promotion)
        sum(case when li.is_on_promotion then 0 else 1 end) = 0 
            and count(*) > 0 as promotion_only_purchase_first_month,
        -- Promotion usage percentage in first month
        round(
            (sum(case when li.is_on_promotion then li.item_count else 0 end) * 100.0) / 
            nullif(sum(li.item_count), 0), 
            2
        ) as promotion_usage_percentage_first_month
    from fact_line_items li
    inner join customer_cohorts c
        on li.customer_key = c.customer_key
    where extract(year from li.order_date) = c.cohort_year
        and extract(month from li.order_date) = c.cohort_month
        {% if is_incremental() %}
        -- Only calculate for customers whose first month is in the incremental window
        and c.first_purchase_date >= (
            select max(month_start_date)
            from {{ this }}
        )
        {% endif %}
    group by li.customer_key
),

-- Final assembly
final as (
    select
        m.customer_key,
        m.year,
        m.month,
        m.quarter,
        m.month_start_date,
        m.month_end_date,
        -- Cohort information
        m.cohort_year,
        m.cohort_month,
        m.cohort_month_label,
        m.first_purchase_date,
        m.months_since_first_purchase,
        -- Monthly activity
        m.purchases_this_month,
        m.revenue_this_month,
        m.basket_value_this_month,
        m.items_purchased_this_month,
        m.items_on_promotion_this_month,
        m.promotion_discount_this_month,
        m.purchases_with_promotions_this_month,
        m.wolt_service_fees_this_month,
        m.courier_fees_this_month,
        m.avg_order_value_this_month,
        m.avg_basket_value_this_month,
        -- Flags
        m.is_first_month,
        -- First month promotion details
        coalesce(f.all_items_on_promotion_first_month, false) as all_items_on_promotion_first_month,
        coalesce(f.has_promotion_items_first_month, false) as has_promotion_items_first_month,
        coalesce(f.promotion_only_purchase_first_month, false) as promotion_only_purchase_first_month,
        f.promotion_usage_percentage_first_month,
        current_timestamp as load_time
    from monthly_with_cohorts m
    left join first_month_promotion_details f
        on m.customer_key = f.customer_key
        and m.is_first_month = true
)

select * from final
order by customer_key, year, month
