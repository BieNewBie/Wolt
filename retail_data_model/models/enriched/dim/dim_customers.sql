{{ config(
    materialized='incremental',
    unique_key='customer_key',
    on_schema_change='append_new_columns',
    post_hook='ANALYZE {{ this }}'
) }}

-- Customer dimension table (Incremental)
-- One row per customer with descriptive attributes only
-- Pure dimensional modeling: no aggregated metrics
-- Derived from stg_purchase_logs

with purchase_logs as (
    select * from {{ ref('stg_purchase_logs') }}
    {% if is_incremental() %}
        where order_date > (select max(last_purchase_date) from {{ this }})
    {% endif %}
),

-- Calculate customer attributes from new purchases
new_customer_attributes as (
    select
        customer_key,
        min(order_date) as first_purchase_date,
        max(order_date) as last_purchase_date
    from purchase_logs
    group by customer_key
),

-- Get existing customers (only on incremental runs)
existing_customers as (
    {% if is_incremental() %}
        select * from {{ this }}
    {% else %}
        select 
            customer_key,
            first_purchase_date,
            last_purchase_date
        from new_customer_attributes
        where 1=0  -- Empty on full refresh
    {% endif %}
),

-- Merge: combine existing and new, recalculate dates
merged_customers as (
    select
        customer_key,
        min(first_purchase_date) as first_purchase_date,
        max(last_purchase_date) as last_purchase_date
    from (
        select 
            customer_key,
            first_purchase_date,
            last_purchase_date
        from existing_customers
        
        union all
        
        select 
            customer_key,
            first_purchase_date,
            last_purchase_date
        from new_customer_attributes
    )
    group by customer_key
)

select
    customer_key,
    first_purchase_date,
    last_purchase_date,
    first_purchase_date as customer_since_date,
    -- Descriptive attributes only - no aggregated metrics
    datediff('day', first_purchase_date, current_date) as days_since_first_purchase,
    datediff('day', last_purchase_date, current_date) as days_since_last_purchase,
    current_timestamp as load_time
from merged_customers

