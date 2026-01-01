# Data Quality Issues & Handling

This document describes known data quality issues in the source data and how they are handled in the transformation models.

## Duplicate Item Logs

### Issue Description

The `snack_store_item_logs` source table contains duplicate records where the same `LOG_ITEM_ID` appears multiple times. About **27% (177 LOG_ITEM_ID)** are affected.

### Root Cause

Duplicate records have identical values for all fields except the `product_base_price` field within the `price_attributes` JSON array. The differences manifest in three patterns:

1. **One record has a price value, the other is NULL** (108 cases - 61%)
   - Example (`LOG_ITEM_ID: 2b57f84f4b7dc29af2aec770304a7b22`): Row 1 has `product_base_price: 1.83`, Row 2 has `product_base_price: null`

2. **Both records have prices but with different values** (69 cases - 39%)
   - Example (`LOG_ITEM_ID: 039519c805aa9ec502b7eb8d0068a044`): Row 1 has `product_base_price: 2.7`, Row 2 has `product_base_price: -2.6418`
   - Note: Some records contain **negative prices**, indicating data quality issues

### Resolution Strategy

The `stg_item_logs` model implements deduplication logic using a `ROW_NUMBER()` window function with the following priority order:

1. **Prioritizes non-null prices** over NULL values
   - Ensures we capture price information when available

2. **Prefers positive prices** over negative prices
   - Handles data quality issues where negative prices appear to be errors
   - Negative prices are likely data entry or transformation errors

3. **Maintains consistent selection** when both records have the same price status
   - Uses `log_item_id` as a tiebreaker for deterministic results

### Implementation

The deduplication is implemented in the `deduplicated` CTE within `stg_item_logs.sql`:

```sql
deduplicated as (
    select
        *,
        row_number() over (
            partition by log_item_id
            order by
                -- Prefer non-null prices
                case when product_base_price is not null then 0 else 1 end,
                -- Prefer positive prices over negative
                case when product_base_price is not null and product_base_price > 0 then 0 else 1 end,
                -- If both have same price status, keep consistent ordering
                log_item_id
        ) as row_num
    from extracted_names
)
```

Only rows where `row_num = 1` are selected in the final output.

### Impact

- The standardized layer contains exactly one clean record per `log_item_id`

## Missing Brand Names (Upstream Data Quality Issue)

### Issue Description

The `stg_item_logs` standardized table contains rows where the `brand_name` field is NULL. **30 rows (6.37%) are affected.**

### Root Cause

NULL `brand_name` values originate from the landing layer source data and are preserved through the standardized layer to `dim_items`. This is an **upstream data quality issue** that should be addressed at the source system level.

### Examples

1. **`item_key: 8daf7c8493c249f143d4d72205f12405`** - "Nippon Häppchen puffed rice with milk chocolate, 200 g" (Cookies)

2. **`item_key: cfda38a00b0aed7f3d0f2dc905553c83`** - "Rapunzel Bionella Vegan Organic Nut Nougat Cream, 400 g" (Chocolate & Sweet Spreads)

### Potential Solution

Item names contain brand-like text (e.g., "Nippon Häppchen", "Rapunzel"), may be `item_name` can be used to extract `brand_name` in such cases, but we will need to align this with producers, if this is the right way for it.

### Data Quality Monitoring

A data quality test has been added to the `stg_item_logs` model in `models/standardized/schema.yml` to monitor this issue:

```yaml
- name: brand_name
  description: "Brand name of the product. Note: Some records have NULL brand_name values (see DATA_QUALITY_ISSUES.md)"
  tests:
    - dbt_utils.expression_is_true:
        expression: "IS NOT NULL"
        config:
          severity: warn
          name: "brand_name_missing"
```

This test will warn (not fail) when `brand_name` is NULL, allowing the pipeline to continue while making the data quality issue visible.
