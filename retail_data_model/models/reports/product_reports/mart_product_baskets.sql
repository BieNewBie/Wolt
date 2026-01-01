{{ config(
    materialized='incremental',
    unique_key='purchase_key',
    on_schema_change='append_new_columns',
    post_hook='ANALYZE {{ this }}'
) }}

-- Product Baskets Mart (Platinum Layer)
-- Grain: One row per purchase with item combination details
-- Purpose: Enable analysts to answer product combination questions with minimal SQL
-- Shows which items are bought together in each purchase

with fact_purchases as (
    select * from {{ ref('fact_purchases') }}
    {% if is_incremental() %}
        where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),

fact_line_items as (
    select * from {{ ref('fact_purchase_line_items') }}
    {% if is_incremental() %}
        where order_date > (select max(order_date) from {{ this }})
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

-- Get purchase-level details
purchase_details as (
    select
        p.purchase_key,
        p.customer_key,
        p.order_date,
        p.total_basket_value,
        p.total_order_value,
        p.total_items,
        p.unique_items,
        p.items_on_promotion_count,
        p.promotion_discount_amount,
        p.has_promotion_items,
        p.delivery_distance_meters,
        p.wolt_service_fee,
        p.courier_base_fee,
        p.total_fees,
        p.time_order_received_utc
    from fact_purchases p
),

-- Get line items with item details for each purchase
line_items_with_details as (
    select
        li.purchase_key,
        li.item_key,
        li.item_count,
        li.is_on_promotion,
        li.discount_amount,
        i.item_name_en,
        i.item_category
    from fact_line_items li
    inner join dim_items i
        on li.item_key = i.item_key
        and li.time_order_received_utc >= i.valid_from
        and li.time_order_received_utc < i.valid_to
),

-- Get distinct categories per purchase
distinct_categories as (
    select distinct
        purchase_key,
        item_category
    from line_items_with_details
),

-- Aggregate line items per purchase (for item lists)
purchase_line_items_agg as (
    select
        li.purchase_key,
        -- Create pipe-separated list of item names
        string_agg(li.item_name_en, ' | ' order by li.item_key) as item_names_list,
        -- Create comma-separated list of item keys
        string_agg(li.item_key::varchar, ',' order by li.item_key) as item_keys_list,
        -- Create pipe-separated list of categories (from distinct categories)
        (select string_agg(dc.item_category, ' | ' order by dc.item_category) 
         from distinct_categories dc 
         where dc.purchase_key = li.purchase_key) as categories_list,
        -- Count items by category
        count(distinct li.item_key) as unique_items_in_basket,
        count(distinct li.item_category) as unique_categories_in_basket,
        sum(case when li.is_on_promotion then 1 else 0 end) as line_items_on_promotion,
        -- Basket composition metrics
        round(avg(li.item_count), 2) as avg_item_quantity_per_line_item,
        max(li.item_count) as max_item_quantity_in_basket
    from line_items_with_details li
    group by li.purchase_key
),

-- Join everything together
purchase_baskets as (
    select
        pd.*,
        {{ delivery_distance_category('pd.delivery_distance_meters') }} as delivery_distance_category,
        pli.item_names_list,
        pli.item_keys_list,
        pli.categories_list,
        pli.unique_items_in_basket,
        pli.unique_categories_in_basket,
        pli.line_items_on_promotion,
        pli.avg_item_quantity_per_line_item,
        pli.max_item_quantity_in_basket,
        -- Basket type classification
        case
            when pli.unique_items_in_basket = 1 then 'Single Item'
            when pli.unique_items_in_basket = 2 then 'Two Items'
            when pli.unique_items_in_basket between 3 and 5 then 'Small Basket'
            when pli.unique_items_in_basket between 6 and 10 then 'Medium Basket'
            else 'Large Basket'
        end as basket_size,
        case
            when pli.unique_categories_in_basket = 1 then 'Single Category'
            when pli.unique_categories_in_basket = 2 then 'Two Categories'
            else 'Multi-Category'
        end as basket_category_diversity
    from purchase_details pd
    left join purchase_line_items_agg pli
        on pd.purchase_key = pli.purchase_key
),

-- Join with customer and date dimensions for additional context
final as (
    select
        pb.*,
        c.first_purchase_date,
        case 
            when pb.order_date > c.first_purchase_date then true 
            else false 
        end as is_repeat_purchase,
        d.year,
        d.quarter,
        d.month,
        d.season,
        d.is_weekend
    from purchase_baskets pb
    left join dim_customers c
        on pb.customer_key = c.customer_key
    left join dim_dates d
        on pb.order_date = d.date
)

select
    -- Purchase identifiers
    purchase_key,
    customer_key,
    order_date,
    
    -- Date attributes
    year,
    quarter,
    month,
    season,
    is_weekend,
    
    -- Customer attributes
    first_purchase_date,
    is_repeat_purchase,
    
    -- Basket composition
    item_names_list,
    item_keys_list,
    categories_list,
    unique_categories_in_basket,
    basket_size,
    basket_category_diversity,
    
    -- Purchase metrics
    total_basket_value,
    total_order_value,
    total_items,
    unique_items,
    items_on_promotion_count,
    promotion_discount_amount,
    has_promotion_items,
    line_items_on_promotion,
    avg_item_quantity_per_line_item,
    max_item_quantity_in_basket,
    
    -- Delivery and fees
    delivery_distance_meters,
    delivery_distance_category,
    wolt_service_fee,
    courier_base_fee,
    total_fees,
    
    -- Timestamp
    time_order_received_utc,
    
    -- Load time
    current_timestamp as load_time
    
from final
order by order_date desc, purchase_key

