{{ config(
    materialized='incremental',
    unique_key='log_item_id',
    on_schema_change='append_new_columns'
) }}

-- Standardized item logs table
-- This model cleans and standardizes the raw item log data
-- Parses JSON payload and extracts English names
-- Materialized as incremental for scalability (frequent item updates)

with source_data as (
    select * from {{ source('landing', 'snack_store_item_logs') }}
    {% if is_incremental() %}
        where replace(TIME_LOG_CREATED_UTC::varchar, ' Z', '+00:00')::timestamptz > (select max(time_log_created_utc) from {{ this }})
    {% endif %}
),

parsed_payload as (
    select
        LOG_ITEM_ID as log_item_id,
        ITEM_KEY as item_key,
        replace(TIME_LOG_CREATED_UTC::varchar, ' Z', '+00:00')::timestamptz as time_log_created_utc,
        json_extract_string(PAYLOAD, '$.brand_name') as brand_name,
        json_extract_string(PAYLOAD, '$.item_category') as item_category,
        json_extract(PAYLOAD, '$.name') as name_array,
        json_extract(PAYLOAD, '$.price_attributes[0].product_base_price')::double as product_price_incl_vat,
        json_extract(PAYLOAD, '$.price_attributes[0].vat_rate_in_percent')::int as vat_rate_percent,
        json_extract_string(PAYLOAD, '$.price_attributes[0].currency') as currency,
        json_extract_string(PAYLOAD, '$.time_item_created_in_source_utc') as time_item_created_source_utc,
        json_extract(PAYLOAD, '$.weight_in_grams')::int as weight_grams,
        json_extract(PAYLOAD, '$.number_of_units')::int as number_of_units
    from source_data
),

extracted_names as (
    select
        *,
        (
            select json_extract_string(name_item.unnest, '$.value')
            from unnest(json_extract(name_array, '$')::json[]) as name_item
            where json_extract_string(name_item.unnest, '$.lang') = 'en'
            limit 1
        ) as item_name_en
    from parsed_payload
),

-- Deduplicate: Handle cases where same LOG_ITEM_ID appears multiple times with different price values
-- Priority: 1) Non-null price over NULL, 2) Positive price over negative, 3) If both NULL, keep first
deduplicated as (
    select
        *,
        row_number() over (
            partition by log_item_id
            order by
                -- Prefer non-null prices
                case when product_price_incl_vat is not null then 0 else 1 end,
                -- Prefer positive prices over negative
                case when product_price_incl_vat is not null and product_price_incl_vat > 0 then 0 else 1 end,
                -- If both have same price status, keep consistent ordering
                log_item_id
        ) as row_num
    from extracted_names
),

-- Round numeric values to 2 decimal places for consistency
rounded_values as (
    select
        log_item_id,
        item_key,
        time_log_created_utc,
        brand_name,
        item_category,
        item_name_en,
        name_array,
        round(product_price_incl_vat, 2) as product_price_incl_vat,
        vat_rate_percent,
        currency,
        weight_grams,
        number_of_units,
        time_item_created_source_utc
    from deduplicated
    where row_num = 1
)

select
    log_item_id,
    item_key,
    time_log_created_utc,
    brand_name,
    item_category,
    coalesce(
        item_name_en,
        -- Fallback: get first name if English not found
        (select json_extract_string(name_item.unnest, '$.value')
         from unnest(json_extract(name_array, '$')::json[]) as name_item
         limit 1)
    ) as item_name_en,
    product_price_incl_vat,
    -- Calculate price excluding VAT: price_incl_vat / (1 + vat_rate/100)
    round((product_price_incl_vat / (1 + vat_rate_percent / 100.0)), 2) as product_price_excl_vat,
    vat_rate_percent,
    currency,
    weight_grams,
    number_of_units,
    replace(time_item_created_source_utc::varchar, ' Z', '+00:00')::timestamptz as time_item_created_source_utc,
    current_timestamp as load_time
from rounded_values

