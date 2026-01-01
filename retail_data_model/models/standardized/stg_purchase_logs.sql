{{ config(
    materialized='incremental',
    unique_key=['purchase_key', 'item_key'],
    on_schema_change='append_new_columns'
) }}

-- Standardized purchase logs table
-- This model cleans and standardizes the raw purchase data
-- Expands basket JSON to individual line items
-- Materialized as incremental for scalability (high-volume transaction data)

with source_data as (
    select * from {{ source('landing', 'snack_store_purchase_logs') }}
    {% if is_incremental() %}
        where replace(TIME_ORDER_RECEIVED_UTC::varchar, ' Z', '+00:00')::timestamptz > (select max(time_order_received_utc) from {{ this }})
    {% endif %}
),

parsed_purchases as (
    select
        replace(TIME_ORDER_RECEIVED_UTC::varchar, ' Z', '+00:00')::timestamptz as time_order_received_utc,
        replace(TIME_ORDER_RECEIVED_UTC::varchar, ' Z', '+00:00')::timestamptz::date as order_date,
        PURCHASE_KEY as purchase_key,
        CUSTOMER_KEY as customer_key,
        DELIVERY_DISTANCE_LINE_METERS::int as delivery_distance_meters,
        WOLT_SERVICE_FEE::double as wolt_service_fee,
        COURIER_BASE_FEE::double as courier_base_fee,
        TOTAL_BASKET_VALUE::double as total_basket_value,
        ITEM_BASKET_DESCRIPTION::json as basket_json
    from source_data
),

expanded_basket as (
    select
        p.*,
        json_extract_string(basket_item.unnest, '$.item_key') as item_key,
        json_extract(basket_item.unnest, '$.item_count')::int as item_count
    from parsed_purchases p,
    unnest(json_extract(basket_json, '$')::json[]) as basket_item
)

select
    purchase_key,
    customer_key,
    time_order_received_utc,
    order_date,
    delivery_distance_meters,
    round(wolt_service_fee, 2) as wolt_service_fee,
    round(courier_base_fee, 2) as courier_base_fee,
    round(total_basket_value, 2) as total_basket_value,
    item_key,
    item_count,
    current_timestamp as load_time
from expanded_basket

