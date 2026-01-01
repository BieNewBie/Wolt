{{ config(
    materialized='table',
    post_hook='ANALYZE {{ this }}'
) }}

-- Category Product Performance Mart (Platinum Layer)
-- Grain: One row per category + product + time period (month)
-- Purpose: Enable analysts to answer category/product performance questions with minimal SQL
-- Supports: Category performance, product performance, star products, trends, consumption patterns

with fact_line_items as (
    select * from {{ ref('fact_purchase_line_items') }}
),

dim_items as (
    select * from {{ ref('dim_items') }}
),

dim_dates as (
    select * from {{ ref('dim_dates') }}
),

-- Join fact with dimensions to get category, product, and date attributes
sales_with_dimensions as (
    select
        li.*,
        i.item_name_en,
        i.item_category,
        i.brand_name,
        i.product_price_incl_vat,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.season
    from fact_line_items li
    inner join dim_items i
        on li.item_key = i.item_key
        and li.time_order_received_utc >= i.valid_from
        and li.time_order_received_utc < i.valid_to
    inner join dim_dates d
        on li.order_date = d.date
),

-- Aggregate by category + product + month
category_product_monthly as (
    select
        item_category,
        item_key,
        item_name_en,
        brand_name,
        year,
        quarter,
        month,
        month_name,
        season,
        -- Revenue metrics
        sum(line_item_value_after_discount) as revenue,
        sum(line_item_value_before_discount) as revenue_before_discount,
        sum(discount_amount) as total_discount_amount,
        -- Quantity metrics
        sum(item_count) as quantity_sold,
        count(distinct purchase_key) as purchase_count,
        count(distinct customer_key) as customer_count,
        -- Promotion metrics
        sum(case when is_on_promotion then item_count else 0 end) as quantity_on_promotion,
        sum(case when is_on_promotion then line_item_value_after_discount else 0 end) as revenue_on_promotion,
        count(distinct case when is_on_promotion then purchase_key end) as purchases_with_promotion,
        -- Price metrics
        round(avg(unit_price_at_purchase), 2) as avg_price,
        round(min(unit_price_at_purchase), 2) as min_price,
        round(max(unit_price_at_purchase), 2) as max_price,
        round(avg(product_price_incl_vat), 2) as avg_base_price
    from sales_with_dimensions
    group by
        item_category,
        item_key,
        item_name_en,
        brand_name,
        year,
        quarter,
        month,
        month_name,
        season
),

-- Calculate period-over-period growth rates using window functions
with_growth_rates as (
    select
        *,
        -- Previous period values (compute once, reuse below)
        lag(revenue) over (
            partition by item_category, item_key 
            order by year, month
        ) as revenue_previous_month,
        lag(quantity_sold) over (
            partition by item_category, item_key 
            order by year, month
        ) as quantity_previous_month,
        lag(customer_count) over (
            partition by item_category, item_key 
            order by year, month
        ) as customer_count_previous_month
    from category_product_monthly
),

-- Calculate growth percentages using precomputed lag values
with_growth_percentages as (
    select
        *,
        -- Growth rates (using precomputed lag values)
        round(((revenue - revenue_previous_month) * 100.0 / nullif(revenue_previous_month, 0)), 2) as revenue_growth_mom_pct,
        round(((quantity_sold - quantity_previous_month) * 100.0 / nullif(quantity_previous_month, 0)), 2) as quantity_growth_mom_pct,
        round(((customer_count - customer_count_previous_month) * 100.0 / nullif(customer_count_previous_month, 0)), 2) as customer_growth_mom_pct
    from with_growth_rates
),

-- Calculate running totals and averages
with_totals as (
    select
        *,
        -- Running totals (till-date)
        sum(revenue) over (
            partition by item_category, item_key 
            order by year, month 
            rows between unbounded preceding and current row
        ) as revenue_till_date,
        sum(quantity_sold) over (
            partition by item_category, item_key 
            order by year, month 
            rows between unbounded preceding and current row
        ) as quantity_till_date,
        sum(customer_count) over (
            partition by item_category, item_key 
            order by year, month 
            rows between unbounded preceding and current row
        ) as customer_count_till_date,
        -- Average revenue per month (till-date)
        round(
            avg(revenue) over (
                partition by item_category, item_key 
                order by year, month 
                rows between unbounded preceding and current row
            ), 
            2
        ) as avg_monthly_revenue_till_date,
        -- Average quantity per month (till-date)
        round(
            avg(quantity_sold) over (
                partition by item_category, item_key 
                order by year, month 
                rows between unbounded preceding and current row
            ), 
            2
        ) as avg_monthly_quantity_till_date
    from with_growth_percentages
),

-- Calculate marketplace metrics (category-relative rankings)
marketplace_metrics_base as (
    select
        *,
        -- Compute category aggregations once
        sum(revenue) over(partition by item_category, year, month) as category_total_revenue,
        sum(revenue_previous_month) over(partition by item_category, year, month) as category_total_revenue_previous_month,
        -- Revenue Percentile: Product's ranking within category (0.0 = lowest, 1.0 = highest)
        percent_rank() over(
            partition by item_category, year, month 
            order by revenue
        ) as revenue_percentile
    from with_totals
),

marketplace_metrics as (
    select
        *,
        -- Category Tide (Weighted Average): Growth rate of total category revenue
        -- This reflects actual money movement, not skewed by small products with high growth %
        round(
            ((category_total_revenue - category_total_revenue_previous_month) * 100.0 
            / nullif(category_total_revenue_previous_month, 0)),
            2
        ) as category_total_growth_pct,
        -- Market Share: Product's percentage of category revenue for that month
        round((revenue * 100.0 / nullif(category_total_revenue, 0)), 2) as market_share_pct
    from marketplace_metrics_base
),

-- Add calculated metrics
final as (
    select
        -- Identifiers
        item_category,
        item_key,
        item_name_en,
        brand_name,
        -- Time period
        year,
        quarter,
        month,
        month_name,
        season,
        -- Revenue metrics
        round(revenue, 2) as revenue,
        round(revenue_before_discount, 2) as revenue_before_discount,
        round(total_discount_amount, 2) as total_discount_amount,
        round((total_discount_amount * 100.0 / nullif(revenue_before_discount, 0)), 2) as discount_percentage,
        -- Quantity metrics
        quantity_sold,
        purchase_count,
        customer_count,
        round((quantity_sold / nullif(purchase_count, 0)), 2) as avg_quantity_per_purchase,
        round((revenue / nullif(quantity_sold, 0)), 2) as revenue_per_unit,
        round((revenue / nullif(customer_count, 0)), 2) as revenue_per_customer,
        -- Promotion metrics
        quantity_on_promotion,
        round(revenue_on_promotion, 2) as revenue_on_promotion,
        purchases_with_promotion,
        round((quantity_on_promotion * 100.0 / nullif(quantity_sold, 0)), 2) as promotion_quantity_percentage,
        round((revenue_on_promotion * 100.0 / nullif(revenue, 0)), 2) as promotion_revenue_percentage,
        round((purchases_with_promotion * 100.0 / nullif(purchase_count, 0)), 2) as promotion_purchase_percentage,
        -- Price metrics
        avg_price,
        min_price,
        max_price,
        avg_base_price,
        -- Growth metrics
        revenue_previous_month,
        quantity_previous_month,
        customer_count_previous_month,
        revenue_growth_mom_pct,
        quantity_growth_mom_pct,
        customer_growth_mom_pct,
        -- Running totals
        round(revenue_till_date, 2) as revenue_till_date,
        quantity_till_date,
        customer_count_till_date,
        avg_monthly_revenue_till_date,
        avg_monthly_quantity_till_date,
        -- Marketplace metrics
        category_total_growth_pct,
        market_share_pct,
        revenue_percentile,
        -- Performance indicators
        -- Growth Trend: Relative momentum compared to category (marketplace-based)
        -- This tells you "what's happening right now" vs performance_category which tells you "what the product is"
        case
            when revenue_previous_month is null then 'New'  -- First month, no comparison possible
            when category_total_growth_pct is null then null  -- Category has no previous month data
            -- Product is growing >15% faster than the category (accelerating ahead)
            when revenue_growth_mom_pct > (category_total_growth_pct + 15) then 'Accelerating'
            -- Product is beating the category average (outperforming)
            when revenue_growth_mom_pct > category_total_growth_pct then 'Outperforming'
            -- Product is slightly behind category (within 15% range) - on track
            when revenue_growth_mom_pct > (category_total_growth_pct - 15) then 'On Track'
            -- Product is significantly losing share vs category
            else 'Decline vs Category'
        end as growth_trend,
        case
            when revenue_previous_month is null then 'New'  -- First month, no comparison possible
            when category_total_growth_pct is null then null  -- Category has no previous month data
            -- 1. STARS: Top 10% by revenue AND growing faster than category average
            when revenue_percentile >= 0.90 and revenue_growth_mom_pct > category_total_growth_pct then 'Star Product'
            -- 2. ANCHORS (Cash Cows): Top 10% by revenue but growing at or below category average
            when revenue_percentile >= 0.90 and revenue_growth_mom_pct <= category_total_growth_pct then 'Anchor (Cash Cow)'
            -- 3. RISING TRENDS: High 'Alpha' (Beating the category by a lot)
            -- Logic: Must be bottom 50% by revenue, have min 10 purchases, AND:
            --   - If category is positive: grow at least 2x category speed
            --   - If category is negative: just be positive (beating the declining category)
            when revenue_percentile < 0.50 and purchase_count >= 10 and (
                (category_total_growth_pct > 0 and revenue_growth_mom_pct >= 2 * category_total_growth_pct) or
                (category_total_growth_pct <= 0 and revenue_growth_mom_pct > 0)
            ) then 'Rising Trend'
            -- 4. UNDERPERFORMERS: Everything else (low ranking with weak/negative growth)
            else 'Underperformer'
        end as performance_category,
        current_timestamp as load_time
    from marketplace_metrics
)

select * from final
order by item_category, item_key, year, month

