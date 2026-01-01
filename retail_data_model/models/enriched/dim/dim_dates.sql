{{ config(
    materialized='table',
    post_hook='ANALYZE {{ this }}'
) }}

-- Date dimension table
-- One row per day for date-based analysis
-- Generated from date range (2022-01-01 to 2025-12-31)
-- Complete date coverage regardless of data availability

with date_range as (
    select
        generate_series as date
    from generate_series(
        date '2022-01-01',
        date '2025-12-31',
        interval 1 day
    )
),

date_attributes as (
    select
        date,
        extract(year from date) as year,
        extract(quarter from date) as quarter,
        extract(month from date) as month,
        extract(week from date) as week,
        extract(dow from date) as day_of_week,
        extract(day from date) as day_of_month,
        extract(doy from date) as day_of_year,
        case 
            when extract(dow from date) in (0, 6) then true 
            else false 
        end as is_weekend,
        case 
            when extract(month from date) in (12, 1, 2) then 'Winter'
            when extract(month from date) in (3, 4, 5) then 'Spring'
            when extract(month from date) in (6, 7, 8) then 'Summer'
            else 'Fall'
        end as season,
        strftime(date, '%B') as month_name,
        strftime(date, '%A') as day_name,
        current_timestamp as load_time
    from date_range
)

select * from date_attributes
order by date

