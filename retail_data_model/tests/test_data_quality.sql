-- Test: Data Quality Checks
-- Validates data quality across the standardized models
-- Only includes tests that are NOT covered by schema.yml files
-- (schema.yml handles: uniqueness, not_null, and basic range checks, but not empty string validation)

with data_quality_issues as (
    -- Test 1: No duplicate purchase_key + item_key combinations in stg_purchase_logs
    -- Each purchase can have multiple items, but each purchase+item combination should be unique
    -- Note: This is not covered by schema.yml (which only tests individual column uniqueness)
    select 'duplicate_purchase_item_combinations' as issue_type, sum(dup_count) as issue_count
    from (
        select purchase_key, item_key, count(*) as dup_count
        from {{ ref('stg_purchase_logs') }}
        group by purchase_key, item_key
        having count(*) > 1
    ) duplicates
    
    union all
    
    -- Test 2: Verify deduplication preserved the correct number of unique records
    -- This is a monitoring test to ensure deduplication is working correctly
    -- Note: This is a data quality monitoring test specific to our deduplication logic
    select 'deduplication_count_mismatch' as issue_type, 
           abs(total_rows - unique_count) as issue_count
    from (
        select 
            count(*) as total_rows,
            count(distinct log_item_id) as unique_count
        from {{ ref('stg_item_logs') }}
    ) counts
    where total_rows != unique_count
    
    union all
    
    -- Test 3: Verify that purchase logs have valid item_key values (empty strings)
    -- Note: schema.yml checks for not_null, but not for empty strings
    select 'empty_item_key_in_purchases' as issue_type, count(*) as issue_count
    from {{ ref('stg_purchase_logs') }}
    where trim(item_key) = ''
    
    union all
    
    -- Test 4: Verify that item logs have valid item_key values (empty strings)
    -- Note: schema.yml checks for not_null, but not for empty strings
    select 'empty_item_key_in_item_logs' as issue_type, count(*) as issue_count
    from {{ ref('stg_item_logs') }}
    where trim(item_key) = ''
    
    union all
    
    -- Test 5: Verify that promos have valid item_key values (empty strings)
    -- Note: schema.yml checks for not_null, but not for empty strings
    select 'empty_item_key_in_promos' as issue_type, count(*) as issue_count
    from {{ ref('stg_promos') }}
    where trim(item_key) = ''
)

select issue_type, issue_count
from data_quality_issues
where issue_count > 0

