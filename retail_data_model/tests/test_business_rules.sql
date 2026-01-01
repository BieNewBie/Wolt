-- Test: Business Rules Validation
-- Validates key business logic rules across the standardized models
-- Only includes tests that are NOT covered by schema.yml files
-- (schema.yml handles: promo dates, discount percentage, item_count, delivery_distance, fees, basket_value)

with business_rule_violations as (
    -- Test 1: Product prices should be reasonable (not excessively high)
    -- Assuming prices are in EUR, flag any prices over 1000 EUR as potentially erroneous
    -- Note: schema.yml checks price > 0, but not upper bound
    select 'excessive_product_price' as rule_violation, count(*) as violation_count
    from {{ ref('stg_item_logs') }}
    where product_price_incl_vat > 1000
    
    union all
    
    -- Test 2: VAT rates should be within valid range (typically 0-30%)
    -- Note: schema.yml only checks not_null, not range validation
    select 'invalid_vat_rate' as rule_violation, count(*) as violation_count
    from {{ ref('stg_item_logs') }}
    where vat_rate_percent < 0
       or vat_rate_percent > 30
    
    union all
    
    -- Test 3: Weight should be positive for items (when not null)
    -- Note: schema.yml doesn't validate weight_grams range
    select 'non_positive_weight' as rule_violation, count(*) as violation_count
    from {{ ref('stg_item_logs') }}
    where weight_grams is not null
      and weight_grams <= 0
    
    union all
    
    -- Test 4: Number of units should be positive (when not null)
    -- Note: schema.yml doesn't validate number_of_units range
    select 'non_positive_units' as rule_violation, count(*) as violation_count
    from {{ ref('stg_item_logs') }}
    where number_of_units is not null
      and number_of_units <= 0
)

select rule_violation, violation_count
from business_rule_violations
where violation_count > 0

