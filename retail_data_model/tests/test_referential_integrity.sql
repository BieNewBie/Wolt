-- Test: Referential Integrity
-- Validates foreign key relationships between standardized models
-- Only includes tests that are NOT covered by schema.yml files
-- (schema.yml handles: not_null checks, but not referential integrity)

with referential_integrity_issues as (
    -- Test 1: All items in purchase logs should exist in item logs
    -- This ensures data consistency - we can't have purchases for items that don't exist
    -- Note: This is a cross-table referential integrity check not covered by schema.yml
    select 'purchase_items_not_in_item_logs' as issue_type, count(*) as issue_count
    from {{ ref('stg_purchase_logs') }} pl
    left join {{ ref('stg_item_logs') }} il
        on pl.item_key = il.item_key
    where il.item_key is null
      and pl.item_key is not null
    
    union all
    
    -- Test 2: All items in promos should exist in item logs
    -- Promotions should only exist for items that are in the catalog
    -- Note: This is a cross-table referential integrity check not covered by schema.yml
    select 'promo_items_not_in_item_logs' as issue_type, count(*) as issue_count
    from {{ ref('stg_promos') }} p
    left join {{ ref('stg_item_logs') }} il
        on p.item_key = il.item_key
    where il.item_key is null
      and p.item_key is not null
)

select issue_type, issue_count
from referential_integrity_issues
where issue_count > 0

