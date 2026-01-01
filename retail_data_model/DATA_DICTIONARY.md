# Data Dictionary

This document provides a comprehensive data dictionary for all tables in the Silver (Standardized), Gold (Enriched), and Platinum (Reports/Marts) layers of the retail data model.

---

## Silver Layer (Standardized)

### `standardized.stg_item_logs`
**Description**: Standardized item logs with cleaned data types and parsed JSON payload

| Column Name | Description |
|------------|-------------|
| `log_item_id` | Primary key - unique identifier for the log entry |
| `item_key` | Unique identifier of the item |
| `time_log_created_utc` | Timestamp when the item was modified |
| `brand_name` | Brand name of the product. Note: Some records have NULL brand_name values (see DATA_QUALITY_ISSUES.md) |
| `item_category` | Product category (e.g., Crisp & Snacks, Chocolate) |
| `item_name_en` | English name of the product |
| `product_price_incl_vat` | Price of the product in Euros (including VAT) |
| `product_price_excl_vat` | Price of the product in Euros (excluding VAT). Calculated as product_price_incl_vat / (1 + vat_rate_percent / 100) |
| `vat_rate_percent` | VAT rate in percentage points |
| `currency` | Currency code |
| `weight_grams` | Weight of the product in grams |
| `number_of_units` | Number of units in the package |
| `time_item_created_source_utc` | Original creation timestamp from source system |
| `load_time` | When this record was loaded |

### `standardized.stg_promos`
**Description**: Standardized promotional campaigns with cleaned data types

| Column Name | Description |
|------------|-------------|
| `promo_start_date` | First date of promotion (inclusive from midnight) |
| `promo_end_date` | First date when discount is no longer applied (exclusive) |
| `item_key` | Unique identifier of the item on promotion |
| `promo_type` | Type of promotion |
| `discount_percentage` | Percentage points reduction during promotional period |
| `load_time` | When this record was loaded |

### `standardized.stg_purchase_logs`
**Description**: Standardized purchase logs with expanded basket items

| Column Name | Description |
|------------|-------------|
| `purchase_key` | Unique identifier of the purchase (part of composite key with item_key) |
| `customer_key` | Unique identifier of the customer |
| `time_order_received_utc` | Timestamp when order and payment received (UTC) |
| `order_date` | Date portion of order timestamp (for efficient date-based filtering) |
| `delivery_distance_meters` | Straight line distance between store and customer in meters |
| `wolt_service_fee` | Fee for placing order through Wolt App |
| `courier_base_fee` | Base fee courier receives for delivery |
| `total_basket_value` | Total value paid for items including discounts, excluding fees |
| `item_key` | Unique identifier of the item purchased |
| `item_count` | Quantity of this item in the purchase |
| `load_time` | When this record was loaded |

---

## Gold Layer (Enriched)

### Dimensions

#### `dimensions.dim_items`
**Description**: Item dimension table with SCD Type 2 history - valid_from and valid_to dates

| Column Name | Description |
|------------|-------------|
| `log_item_id` | Primary key - unique identifier for the log entry |
| `item_key` | Unique identifier of the item |
| `valid_from` | Start date/time when this version is valid |
| `valid_to` | End date/time when this version is valid (9999-12-31 for current) |
| `is_current` | Whether this is the current version of the item |
| `brand_name` | Brand name of the product |
| `item_category` | Product category (e.g., Crisp & Snacks, Chocolate) |
| `item_name_en` | English name of the product |
| `product_price_incl_vat` | Price of the product in Euros (including VAT) |
| `product_price_excl_vat` | Price of the product in Euros (excluding VAT). Calculated as product_price_incl_vat / (1 + vat_rate_percent / 100) |
| `vat_rate_percent` | VAT rate in percentage points |
| `currency` | Currency code |
| `weight_grams` | Weight of the product in grams |
| `number_of_units` | Number of units in the package |
| `time_item_created_source_utc` | Original creation timestamp from source system |
| `load_time` | When this record was loaded |

#### `dimensions.dim_promotions`
**Description**: Promotions dimension with date range logic for joining

| Column Name | Description |
|------------|-------------|
| `promo_start_date` | First date of promotion (inclusive) |
| `promo_end_date` | Original end date from source data (inclusive) |
| `promo_end_date_exclusive` | Calculated exclusive end date for date range joins (promo_end_date - 1 day) |
| `item_key` | Unique identifier of the item on promotion |
| `promo_type` | Type of promotion |
| `discount_percentage` | Percentage points reduction during promotional period |
| `load_time` | When this record was loaded |

#### `dimensions.dim_customers`
**Description**: Customer dimension table with descriptive attributes only - no aggregated metrics

| Column Name | Description |
|------------|-------------|
| `customer_key` | Primary key - unique identifier of the customer |
| `first_purchase_date` | Date of first purchase |
| `last_purchase_date` | Date of last purchase |
| `customer_since_date` | Date customer first made a purchase |
| `days_since_first_purchase` | Days since first purchase (descriptive attribute) |
| `days_since_last_purchase` | Days since last purchase (descriptive attribute) |
| `load_time` | When this record was loaded |

#### `dimensions.dim_dates`
**Description**: Time dimension table for date-based analysis

| Column Name | Description |
|------------|-------------|
| `date` | Primary key - date |
| `year` | Year |
| `quarter` | Quarter (1-4) |
| `month` | Month (1-12) |
| `week` | Week number |
| `day_of_week` | Day of week (0=Sunday, 6=Saturday) |
| `day_of_month` | Day of month (1-31) |
| `day_of_year` | Day of year (1-366) |
| `is_weekend` | Whether the date is a weekend |
| `season` | Season (Winter, Spring, Summer, Fall) |
| `month_name` | Full name of the month |
| `day_name` | Full name of the day of week |
| `load_time` | When this record was loaded |

### Facts

#### `facts.fact_purchase_line_items`
**Description**: Fact table: Purchase line items - one row per item in each purchase

| Column Name | Description |
|------------|-------------|
| `purchase_key` | Foreign key to purchase |
| `customer_key` | Foreign key to customer dimension |
| `item_key` | Foreign key to item dimension |
| `order_date` | Foreign key to time dimension (date) |
| `item_count` | Quantity of items (measure) |
| `unit_price_at_purchase` | Unit price of item at time of purchase (measure) |
| `line_item_value_before_discount` | Line item value before discount (measure) |
| `line_item_value_after_discount` | Line item value after discount (measure) |
| `discount_amount` | Discount amount applied (measure) |
| `is_on_promotion` | Whether item was on promotion at purchase time |
| `delivery_distance_meters` | Delivery distance in meters (degenerate dimension) |
| `wolt_service_fee` | Wolt service fee (degenerate dimension) |
| `courier_base_fee` | Courier base fee (degenerate dimension) |
| `total_basket_value` | Total basket value (degenerate dimension) |
| `time_order_received_utc` | Timestamp when order was received (degenerate dimension) |
| `load_time` | When this record was loaded |

#### `facts.fact_purchases`
**Description**: Fact table: Purchases - one row per purchase

| Column Name | Description |
|------------|-------------|
| `purchase_key` | Primary key - unique identifier of the purchase |
| `customer_key` | Foreign key to customer dimension |
| `order_date` | Foreign key to time dimension (date) |
| `total_basket_value` | Total basket value (measure) |
| `total_basket_value_before_discounts` | Total basket value before discounts (calculated measure - sum of line_item_value_before_discount) |
| `total_items` | Total number of items (measure) |
| `unique_items` | Number of unique items in purchase (measure) |
| `items_on_promotion_count` | Count of items on promotion (measure) |
| `promotion_discount_amount` | Total promotion discount amount (measure) |
| `wolt_service_fee` | Wolt service fee (measure) |
| `courier_base_fee` | Courier base fee (measure) |
| `total_order_value` | Total order value including fees (measure) |
| `avg_item_price` | Average item price (calculated measure) |
| `total_fees` | Total fees (calculated measure) |
| `fees_as_percent_of_basket` | Fees as percentage of basket (calculated measure) |
| `has_promotion_items` | Whether purchase contains items on promotion |
| `delivery_distance_meters` | Delivery distance in meters (degenerate dimension) |
| `time_order_received_utc` | Timestamp when order was received (degenerate dimension) |
| `load_time` | When this record was loaded |

---

## Platinum Layer (Reports/Marts)

### Sales Reports

#### `marts.mart_sales_summary`
**Description**: Sales Summary Mart - Pre-joined fact and dimension tables for easy analytics. Grain: One row per purchase line item with all dimensions pre-joined.

| Column Name | Description |
|------------|-------------|
| `purchase_key` | Primary key - unique purchase identifier |
| `customer_key` | Foreign key to customer dimension |
| `item_key` | Foreign key to item dimension |
| `order_date` | Date of the order |
| `year` | Year of the order |
| `quarter` | Quarter (1-4) |
| `month` | Month (1-12) |
| `week` | Week number |
| `day_of_week` | Day of week (0=Sunday, 6=Saturday) |
| `day_of_month` | Day of month (1-31) |
| `day_of_year` | Day of year (1-366) |
| `is_weekend` | Whether the order date is a weekend |
| `season` | Season (Winter, Spring, Summer, Fall) |
| `month_name` | Full name of the month |
| `day_name` | Full name of the day of week |
| `item_name_en` | Item name in English |
| `item_category` | Product category |
| `brand_name` | Brand name of the item |
| `product_price_incl_vat` | Product price including VAT |
| `product_price_excl_vat` | Product price excluding VAT |
| `vat_rate_percent` | VAT rate percentage |
| `currency` | Currency code |
| `weight_grams` | Weight of the item in grams |
| `item_is_current` | Whether this is the current version of the item (SCD Type 2) |
| `first_purchase_date` | Customer's first purchase date |
| `last_purchase_date` | Customer's last purchase date |
| `customer_since_date` | Date customer first made a purchase |
| `days_since_first_purchase` | Days since customer's first purchase |
| `days_since_last_purchase` | Days since customer's last purchase |
| `is_first_purchase` | Whether this is the customer's first purchase |
| `is_on_promotion` | Whether this line item was on promotion |
| `promo_type` | Type of promotion applied |
| `promo_discount_percentage` | Discount percentage of the promotion |
| `promo_start_date` | Promotion start date |
| `promo_end_date` | Promotion end date |
| `item_count` | Number of items in this line item |
| `unit_price_at_purchase` | Unit price at time of purchase |
| `line_item_value_before_discount` | Line item value before discount |
| `line_item_value_after_discount` | Line item value after discount (including VAT) |
| `discount_amount` | Discount amount applied to this line item |
| `delivery_distance_meters` | Delivery distance in meters |
| `delivery_distance_category` | Delivery distance category (Very Close, Close, Medium, Far) |
| `wolt_service_fee` | Wolt service fee for this purchase |
| `courier_base_fee` | Courier base fee for this purchase |
| `total_basket_value` | Total basket value for the purchase |
| `time_order_received_utc` | Timestamp when order was received (UTC) |
| `total_fees` | Total fees (wolt_service_fee + courier_base_fee) |
| `fees_as_percent_of_basket` | Fees as percentage of basket value |
| `load_time` | When this record was loaded |

### Customer Reports

#### `marts.mart_customer_summary`
**Description**: Customer Summary Mart - Customer-level aggregated metrics. Grain: One row per customer with aggregated purchase metrics across their entire lifetime.

| Column Name | Description |
|------------|-------------|
| `customer_key` | Primary key - unique customer identifier |
| `days_since_first_purchase` | Days since first purchase |
| `days_since_last_purchase` | Days since last purchase |
| `total_purchases` | Total number of purchases made by customer |
| `first_purchase_date` | Date of customer's first purchase |
| `last_purchase_date` | Date of customer's last purchase |
| `customer_lifetime_days` | Customer lifetime in days |
| `customer_segment` | Customer segment based on purchase frequency (quartile-based thresholds: 1 purchase, 2-25 (P25), 26-40 (P50), 41-63 (P75), 64+ (above P75)) |
| `customer_value_segment` | Customer value segment based on total revenue (quartile-based thresholds: €0, €0.01-190 (P25), €191-310 (P50), €311-490 (P75), €491+ (above P75)) |
| `total_revenue` | Total revenue from customer (total_order_value across all purchases) |
| `total_basket_value` | Total basket value across all purchases |
| `total_wolt_service_fees` | Total Wolt service fees across all purchases |
| `total_courier_fees` | Total courier fees across all purchases |
| `total_fees` | Total fees (wolt + courier) across all purchases - sourced from fact_purchases where it's calculated as wolt_service_fee + courier_base_fee |
| `avg_order_value` | Average order value for the customer |
| `avg_basket_value` | Average basket value for the customer |
| `total_items_purchased` | Total items purchased across all purchases |
| `avg_items_per_order` | Average items per order |
| `total_items_on_promotion` | Total items on promotion across all purchases |
| `total_promotion_discounts` | Total promotion discount amount across all purchases |
| `purchases_with_promotions` | Number of purchases that included promotions |
| `promotion_usage_percentage` | Percentage of purchases that included promotions |
| `items_on_promotion_percentage` | Percentage of items purchased that were on promotion |
| `discount_savings_percentage` | Discount savings as percentage of basket value |
| `avg_fees_as_percent_of_basket` | Average fees as percentage of basket value |
| `load_time` | When this record was loaded |

#### `marts.mart_customer_monthly_metrics`
**Description**: Customer Monthly Metrics (Base Table) - Monthly metrics only, no running totals. Grain: One row per customer per month with monthly aggregated metrics. Important: This model only includes months where customers made purchases.

| Column Name | Description |
|------------|-------------|
| `customer_key` | Foreign key to customer dimension |
| `year` | Year |
| `month` | Month number |
| `quarter` | Quarter (1-4) |
| `month_start_date` | Start date of the analysis month |
| `month_end_date` | End date of the analysis month |
| `cohort_year` | Year of customer's first purchase |
| `cohort_month` | Month of customer's first purchase |
| `cohort_month_label` | Cohort month in YYYY-MM format (first purchase month) |
| `first_purchase_date` | Date of customer's first purchase |
| `months_since_first_purchase` | Number of months since first purchase |
| `purchases_this_month` | Number of purchases made this month |
| `revenue_this_month` | Revenue generated this month |
| `basket_value_this_month` | Basket value (excluding fees) this month |
| `items_purchased_this_month` | Total items purchased this month |
| `items_on_promotion_this_month` | Count of items on promotion this month |
| `promotion_discount_this_month` | Total promotion discount amount this month |
| `purchases_with_promotions_this_month` | Number of purchases with promotions this month |
| `wolt_service_fees_this_month` | Wolt service fees this month |
| `courier_fees_this_month` | Courier fees this month |
| `avg_order_value_this_month` | Average order value this month |
| `avg_basket_value_this_month` | Average basket value this month |
| `is_first_month` | Whether this is the customer's first month |
| `all_items_on_promotion_first_month` | Whether all items in first month were on promotion |
| `has_promotion_items_first_month` | Whether any items in first month were on promotion |
| `promotion_only_purchase_first_month` | Whether first-time customer only bought items on promotion in first month |
| `promotion_usage_percentage_first_month` | Promotion usage percentage in first month (NULL for months after first month) |
| `load_time` | When this record was loaded |

#### `marts.mart_customer_monthly_summary`
**Description**: Customer Monthly Summary Mart - Customer behavior over time with monthly granularity, running totals, and segments. Grain: One row per customer per month with monthly metrics, running totals, and customer segments. Important: This model only includes months where customers made purchases.

| Column Name | Description |
|------------|-------------|
| `customer_key` | Foreign key to customer dimension |
| `year` | Year |
| `month` | Month number |
| `quarter` | Quarter (1-4) |
| `month_start_date` | Start date of the analysis month |
| `month_end_date` | End date of the analysis month |
| `cohort_year` | Year of customer's first purchase |
| `cohort_month` | Month of customer's first purchase |
| `cohort_month_label` | Cohort month in YYYY-MM format (first purchase month) |
| `first_purchase_date` | Date of customer's first purchase |
| `months_since_first_purchase` | Number of months since first purchase |
| `purchases_this_month` | Number of purchases made this month |
| `revenue_this_month` | Revenue generated this month |
| `basket_value_this_month` | Basket value (excluding fees) this month |
| `items_purchased_this_month` | Total items purchased this month |
| `items_on_promotion_this_month` | Count of items on promotion this month |
| `promotion_discount_this_month` | Total promotion discount amount this month |
| `purchases_with_promotions_this_month` | Number of purchases with promotions this month |
| `wolt_service_fees_this_month` | Wolt service fees this month |
| `courier_fees_this_month` | Courier fees this month |
| `avg_order_value_this_month` | Average order value this month |
| `avg_basket_value_this_month` | Average basket value this month |
| `is_first_month` | Whether this is the customer's first month |
| `all_items_on_promotion_first_month` | Whether all items in first month were on promotion |
| `has_promotion_items_first_month` | Whether any items in first month were on promotion |
| `promotion_only_purchase_first_month` | Whether first-time customer only bought items on promotion in first month |
| `promotion_usage_percentage_first_month` | Promotion usage percentage in first month (NULL for months after first month) |
| `total_purchases_till_date` | Total purchases till date (running total) |
| `total_revenue_till_date` | Total revenue till date (running total) |
| `total_basket_value_till_date` | Total basket value till date (running total) |
| `total_items_till_date` | Total items purchased till date (running total) |
| `total_items_on_promotion_till_date` | Total items on promotion till date (running total) |
| `total_promotion_discount_till_date` | Total promotion discount till date (running total) |
| `total_purchases_with_promotions_till_date` | Total purchases with promotions till date (running total) |
| `total_wolt_service_fees_till_date` | Total Wolt service fees till date (running total) |
| `total_courier_fees_till_date` | Total courier fees till date (running total) |
| `promotion_usage_percentage_till_date` | Percentage of purchases with promotions till date |
| `items_on_promotion_percentage_till_date` | Percentage of items on promotion till date |
| `discount_savings_percentage_till_date` | Discount savings as percentage of basket value till date |
| `customer_segment_at_end_of_month` | Customer segment at month end based on total purchases till date (quartile-based thresholds: No Purchases (0), One-Time (1), Occasional (2-25), Regular (26-40), Frequent (41-63), Very Frequent (64+)) |
| `load_time` | When this record was loaded |

### Product Reports

#### `marts.mart_product_baskets`
**Description**: Product Baskets Mart - Purchase-level with item combination details. Grain: One row per purchase with item lists and combination details.

| Column Name | Description |
|------------|-------------|
| `purchase_key` | Primary key - unique purchase identifier |
| `customer_key` | Foreign key to customer dimension |
| `order_date` | Date of the order |
| `year` | Year of the order |
| `quarter` | Quarter (1-4) |
| `month` | Month (1-12) |
| `season` | Season (Winter, Spring, Summer, Fall) |
| `is_weekend` | Whether the order date is a weekend |
| `first_purchase_date` | Customer's first purchase date |
| `is_repeat_purchase` | Whether this is a repeat purchase for the customer |
| `item_names_list` | Pipe-separated list of item names in the basket (format: 'Item1 \| Item2 \| Item3'). Use for human-readable basket inspection and exact basket matching. |
| `item_keys_list` | Comma-separated list of item keys in the basket (format: '1,2,3'). Use for programmatic basket analysis, exact basket matching, and creating item pairs for co-occurrence analysis. |
| `categories_list` | Pipe-separated list of categories in the basket (alphabetically sorted, format: 'Category1 \| Category2 \| Category3'). Use for category diversity analysis and cross-category purchase patterns. |
| `unique_categories_in_basket` | Number of unique categories in the basket |
| `basket_size` | Basket size category (Single Item, Two Items, Small Basket (3-5 items), Medium Basket (6-10 items), Large Basket (11+ items)) |
| `basket_category_diversity` | Basket category diversity (Single Category, Two Categories, Multi-Category (3+ categories)) |
| `total_basket_value` | Total basket value for the purchase (excluding fees) |
| `total_order_value` | Total order value (basket value + fees) |
| `total_items` | Total items in the purchase (sum of all item quantities) |
| `unique_items` | Number of unique items in the purchase (distinct item_key count) |
| `items_on_promotion_count` | Number of items on promotion in this purchase (from fact_purchases) |
| `promotion_discount_amount` | Total promotion discount amount for this purchase (from fact_purchases) |
| `has_promotion_items` | Whether this purchase has items on promotion (from fact_purchases) |
| `line_items_on_promotion` | Count of line items that were on promotion |
| `avg_item_quantity_per_line_item` | Average item quantity per line item in the basket |
| `max_item_quantity_in_basket` | Maximum item quantity of any single item in the basket |
| `delivery_distance_meters` | Delivery distance in meters |
| `delivery_distance_category` | Delivery distance category (Very Close, Close, Medium, Far) - calculated using delivery_distance_category macro |
| `wolt_service_fee` | Wolt service fee for this purchase |
| `courier_base_fee` | Courier base fee for this purchase |
| `total_fees` | Total fees (wolt_service_fee + courier_base_fee) |
| `time_order_received_utc` | Timestamp when order was received (UTC) |
| `load_time` | When this record was loaded |

#### `marts.mart_category_product_performance`
**Description**: Category Product Performance Mart - Pre-aggregated category and product performance metrics. Grain: One row per category + product + time period (month).

| Column Name | Description |
|------------|-------------|
| `item_category` | Product category |
| `item_key` | Product identifier |
| `item_name_en` | Product name in English |
| `brand_name` | Brand name of the item |
| `year` | Year |
| `quarter` | Quarter (1-4) |
| `month` | Month (1-12) |
| `month_name` | Full name of the month |
| `season` | Season (Winter, Spring, Summer, Fall) |
| `revenue` | Revenue for this category+product+month |
| `revenue_before_discount` | Revenue before discount |
| `total_discount_amount` | Total discount amount |
| `discount_percentage` | Discount percentage |
| `quantity_sold` | Quantity sold this month |
| `purchase_count` | Number of purchases this month |
| `customer_count` | Number of unique customers who purchased this month |
| `avg_quantity_per_purchase` | Average quantity per purchase |
| `revenue_per_unit` | Revenue per unit sold |
| `revenue_per_customer` | Revenue per customer |
| `quantity_on_promotion` | Quantity sold on promotion |
| `revenue_on_promotion` | Revenue from items on promotion |
| `purchases_with_promotion` | Number of purchases with promotions |
| `promotion_quantity_percentage` | Percentage of quantity sold on promotion |
| `promotion_revenue_percentage` | Percentage of revenue from promotions |
| `promotion_purchase_percentage` | Percentage of purchases with promotions |
| `avg_price` | Average price at purchase |
| `min_price` | Minimum price at purchase |
| `max_price` | Maximum price at purchase |
| `avg_base_price` | Average base price |
| `revenue_previous_month` | Revenue in previous month |
| `quantity_previous_month` | Quantity sold in previous month |
| `customer_count_previous_month` | Customer count in previous month |
| `revenue_growth_mom_pct` | Month-over-month revenue growth percentage |
| `quantity_growth_mom_pct` | Month-over-month quantity growth percentage |
| `customer_growth_mom_pct` | Month-over-month customer growth percentage |
| `revenue_till_date` | Total revenue till date (running total) |
| `quantity_till_date` | Total quantity sold till date (running total) |
| `customer_count_till_date` | Total customer count till date (running total) |
| `avg_monthly_revenue_till_date` | Average monthly revenue till date |
| `avg_monthly_quantity_till_date` | Average monthly quantity till date |
| `category_total_growth_pct` | Category total growth percentage (weighted average - growth of total category revenue). Reflects actual money movement in the category, not skewed by small products with high growth percentages. NULL when category has no previous month data. |
| `market_share_pct` | Product's market share percentage within its category for that month (product revenue / total category revenue * 100) |
| `revenue_percentile` | Product's revenue percentile ranking within its category for that month (0.0 = lowest, 1.0 = highest). Used for marketplace-based performance classification. |
| `growth_trend` | Relative growth momentum compared to category (marketplace-based). Values: 'New' (first month), 'Accelerating' (>15% faster than category), 'Outperforming' (beating category average), 'On Track' (within 15% of category), 'Decline vs Category' (significantly losing share). NULL when category has no previous month data. This tells you 'what's happening right now' vs performance_category which tells you 'what the product is'. |
| `performance_category` | Marketplace-based performance category: 'New' (first month, no comparison possible), Star Product (top 10% revenue AND growing faster than category), Anchor (Cash Cow) (top 10% revenue but growing at/below category average), Rising Trend (bottom 50% revenue but growing 2x+ category speed with min 10 purchases), Underperformer (everything else). NULL when category has no previous month data. |
| `load_time` | When this record was loaded |

---

## Notes

- All monetary values are rounded to 2 decimal places for consistency
- All timestamps are in UTC
- `load_time` indicates when the record was loaded into the table
- For SCD Type 2 dimensions (e.g., `dim_items`), use `is_current = true` to get the current version
- For date range joins with promotions, use `promo_end_date_exclusive` in `dim_promotions`
- Some columns may have NULL values - refer to individual column descriptions for details
- For data quality issues, see [DATA_QUALITY_ISSUES.md](DATA_QUALITY_ISSUES.md)

