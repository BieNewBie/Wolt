# Business Questions Queries

This document provides exact SQL queries to answer all business questions from the assignment using the data marts.

---

## Task 1 Questions

### 1. What area is the store serving?

**Description**: Analyze delivery distance categories to understand the geographic area the store serves

**Mart**: `marts.mart_sales_summary`

**Query**:
```sql
SELECT 
    delivery_distance_category,
    COUNT(DISTINCT purchase_key) as total_purchases,
    COUNT(*) as total_line_items,
    ROUND(AVG(delivery_distance_meters), 2) as avg_distance_meters,
    ROUND(SUM(total_basket_value), 2) as total_revenue
FROM marts.mart_sales_summary
GROUP BY delivery_distance_category
ORDER BY total_purchases DESC;
```

**Results** (first 5 rows):

| delivery_distance_category | total_purchases | total_line_items | avg_distance_meters | total_revenue |
|---|---|---|---|---|
| Very Close | 57204 | 72985 | 1523.32 | 388571.48 |
| Medium | 27412 | 34942 | 7571.5 | 183109.73 |
| Close | 14255 | 18095 | 3548.66 | 97706.42 |

*Total rows returned: 3*

**Insight**: The store primarily serves 'Very Close' areas, accounting for 57.9% of all purchases (57,204 out of 98,871 total purchases).

---

### 2. What items are being bought and what price?

**Description**: Get items purchased with their prices, filtered by category and date

**Mart**: `marts.mart_sales_summary`

**Query**:
```sql
SELECT 
    item_category,
    item_name_en,
    ROUND(AVG(unit_price_at_purchase), 2) as avg_price,
    ROUND(MIN(unit_price_at_purchase), 2) as min_price,
    ROUND(MAX(unit_price_at_purchase), 2) as max_price,
    SUM(item_count) as total_quantity_sold,
    COUNT(DISTINCT purchase_key) as purchase_count
FROM marts.mart_sales_summary
WHERE order_date >= '2023-01-01'
GROUP BY item_category, item_name_en
ORDER BY total_quantity_sold DESC
LIMIT 20;
```

**Results** (first 5 rows):

| item_category | item_name_en | avg_price | min_price | max_price | total_quantity_sold | purchase_count |
|---|---|---|---|---|---|---|
| Crisp & Snacks | Funny-Frisch Kettle Chips Sweet Chilli & Red Pepper, 120 g | 2.61 | 1.81 | 2.69 | 43096 | 33349 |
| Chocolate | Tony’s Chocolonely Dark Milk Brownie Chocolate, 180 g | 3.51 | 2.5 | 3.65 | 25447 | 15409 |
| Other Confectionary | Tony’s Chocolonely Dark Milk Brownie Chocolate, 180 g | 3.37 | 3.29 | 3.46 | 22638 | 14982 |
| Cheese Crackers, Breadsticks & Dipping | Fuego salsa dip, mild, 200 ml | 2.73 | 2.6 | 2.74 | 8613 | 7386 |
| Chocolate | Yogurette Strawberry Yogurt Chocolate, 100 g | 1.32 | 1.0 | 1.49 | 2928 | 2124 |

*Total rows returned: 20*

**Insight**: 'Funny-Frisch Kettle Chips Sweet Chilli & Red Pepper, 120 g' is the top-selling item with 43,096 units sold at an average price of €2.61.

---

### 3. How many items on promotion?

**Description**: Count items on promotion by time period

**Mart**: `marts.mart_sales_summary`

**Query**:
```sql
SELECT 
    year,
    quarter,
    month,
    month_name,
    SUM(CASE WHEN is_on_promotion THEN item_count ELSE 0 END) as items_on_promotion,
    COUNT(CASE WHEN is_on_promotion THEN 1 END) as line_items_on_promotion,
    COUNT(DISTINCT CASE WHEN is_on_promotion THEN purchase_key END) as purchases_with_promotions,
    ROUND(SUM(CASE WHEN is_on_promotion THEN item_count ELSE 0 END) * 100.0 / 
          NULLIF(SUM(item_count), 0), 2) as promotion_percentage
FROM marts.mart_sales_summary
GROUP BY year, quarter, month, month_name
ORDER BY year, quarter, month;
```

**Results** (first 5 rows):

| year | quarter | month | month_name | items_on_promotion | line_items_on_promotion | purchases_with_promotions | promotion_percentage |
|---|---|---|---|---|---|---|---|
| 2022 | 4 | 12 | December | 10 | 10 | 10 | 0.91 |
| 2023 | 1 | 1 | January | 159 | 142 | 139 | 1.82 |
| 2023 | 1 | 2 | February | 93 | 86 | 83 | 0.99 |
| 2023 | 1 | 3 | March | 296 | 247 | 247 | 2.38 |
| 2023 | 2 | 4 | April | 200 | 180 | 178 | 1.83 |

*Total rows returned: 14*

**Insight**: Promotions account for an average of 4.56% of items sold across all periods, with 9,287 total items sold on promotion.

---

### 4. Are customers taking advantage of promotions?

**Description**: Analyze customer promotion usage patterns

**Mart**: `marts.mart_customer_summary`

**Query**:
```sql
SELECT 
    CASE 
        WHEN promotion_usage_percentage = 0 THEN 'No Promo Usage'
        WHEN promotion_usage_percentage < 25 THEN 'Low Promo Usage (1-24%)'
        WHEN promotion_usage_percentage < 50 THEN 'Medium Promo Usage (25-49%)'
        WHEN promotion_usage_percentage < 75 THEN 'High Promo Usage (50-74%)'
        ELSE 'Very High Promo Usage (75%+)'
    END as promo_usage_category,
    COUNT(*) as customer_count,
    ROUND((SUM(purchases_with_promotions) / NULLIF(SUM(total_purchases), 0)) * 100, 2) as avg_promo_usage_pct,
    ROUND(AVG(total_revenue), 2) as avg_revenue,
    ROUND(AVG(total_purchases), 2) as avg_purchases
FROM marts.mart_customer_summary
GROUP BY promo_usage_category
ORDER BY avg_promo_usage_pct DESC;
```

**Results** (first 5 rows):

| promo_usage_category | customer_count | avg_promo_usage_pct | avg_revenue | avg_purchases |
|---|---|---|---|---|
| Medium Promo Usage (25-49%) | 16 | 29.24 | 214.83 | 28.0 |
| Low Promo Usage (1-24%) | 1673 | 6.51 | 423.7 | 54.16 |
| No Promo Usage | 312 | 0.0 | 185.75 | 25.04 |

*Total rows returned: 3*

**Insight**: Most customers (83.6% or 1,673 out of 2,001) have low promotion usage (1-24%), indicating promotions are not heavily relied upon by the majority of customers.

---

### 5. Are customers coming back?

**Description**: Analyze customer retention and repeat purchase behavior

**Mart**: `marts.mart_customer_summary`

**Query**:
```sql
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(total_purchases), 2) as avg_purchases,
    ROUND(AVG(total_revenue), 2) as avg_revenue,
    ROUND(AVG(days_since_last_purchase), 2) as avg_days_since_last_purchase,
    COUNT(CASE WHEN total_purchases > 1 THEN 1 END) as repeat_customers,
    ROUND(COUNT(CASE WHEN total_purchases > 1 THEN 1 END) * 100.0 / COUNT(*), 2) as repeat_customer_percentage
FROM marts.mart_customer_summary
GROUP BY customer_segment
ORDER BY 
    CASE customer_segment
        WHEN 'One-Time Customer' THEN 1
        WHEN 'Occasional Customer' THEN 2
        WHEN 'Regular Customer' THEN 3
        WHEN 'Frequent Customer' THEN 4
        WHEN 'Very Frequent Customer' THEN 5
        ELSE 0
    END;
```

**Results** (first 5 rows):

| customer_segment | customer_count | avg_purchases | avg_revenue | avg_days_since_last_purchase | repeat_customers | repeat_customer_percentage |
|---|---|---|---|---|---|---|
| Occasional Customer | 516 | 17.81 | 135.57 | 734.97 | 516 | 100.0 |
| Regular Customer | 501 | 32.93 | 253.66 | 729.91 | 501 | 100.0 |
| Frequent Customer | 488 | 50.94 | 391.07 | 726.27 | 488 | 100.0 |
| Very Frequent Customer | 496 | 97.42 | 770.88 | 723.42 | 496 | 100.0 |

*Total rows returned: 4*

**Insight**: Customer retention is strong with 100.0% repeat customer rate across all segments, totaling 2,001 customers distributed across 4 frequency-based segments.

**Note**: The current condition `total_purchases > 1` to define repeat_customers can be more refined as per business logic, in that case the repeat_customers_percentage will be different, tailored to business needs.

---

### 6. How do fees compare to basket value?

**Description**: Analyze fees as percentage of basket value

**Mart**: `marts.mart_sales_summary`

**Query**:
```sql
SELECT 
    year,
    month,
    month_name,
    ROUND((SUM(total_fees) / NULLIF(SUM(total_basket_value), 0)) * 100, 2) as avg_fees_percent_of_basket,
    ROUND(AVG(total_basket_value), 2) as avg_basket_value,
    ROUND(AVG(total_fees), 2) as avg_fees,
    COUNT(DISTINCT purchase_key) as purchase_count
FROM marts.mart_sales_summary
GROUP BY year, month, month_name
ORDER BY year, month;
```

**Results** (first 5 rows):

| year | month | month_name | avg_fees_percent_of_basket | avg_basket_value | avg_fees | purchase_count |
|---|---|---|---|---|---|---|
| 2022 | 12 | December | 62.46 | 5.33 | 3.33 | 667 |
| 2023 | 1 | January | 62.83 | 5.25 | 3.3 | 5333 |
| 2023 | 2 | February | 63.59 | 5.17 | 3.29 | 5722 |
| 2023 | 3 | March | 63.06 | 5.2 | 3.28 | 7666 |
| 2023 | 4 | April | 60.33 | 5.42 | 3.27 | 6685 |

*Total rows returned: 14*

**Insight**: Fees represent an average of 62.24% of basket value across all months, indicating fees are a significant portion of the total order value relative to product costs.


---

### 7. How much revenue generated?

**Description**: Calculate total revenue by time period

**Mart**: `marts.mart_sales_summary`

**Query**:
```sql
SELECT 
    year,
    month,
    month_name,
    ROUND(SUM(line_item_value_after_discount), 2) as total_revenue,
    COUNT(DISTINCT purchase_key) as total_purchases,
    COUNT(DISTINCT customer_key) as unique_customers,
    ROUND(SUM(line_item_value_after_discount) / NULLIF(COUNT(DISTINCT purchase_key), 0), 2) as avg_revenue_per_purchase
FROM marts.mart_sales_summary
GROUP BY year, month, month_name
ORDER BY year, month;
```

**Results** (first 5 rows):

| year | month | month_name | total_revenue | total_purchases | unique_customers | avg_revenue_per_purchase |
|---|---|---|---|---|---|---|
| 2022 | 12 | December | 3167.11 | 667 | 467 | 4.75 |
| 2023 | 1 | January | 23717.15 | 5333 | 1456 | 4.45 |
| 2023 | 2 | February | 25011.8 | 5722 | 1446 | 4.37 |
| 2023 | 3 | March | 33390.11 | 7666 | 1594 | 4.36 |
| 2023 | 4 | April | 29627.09 | 6685 | 1568 | 4.43 |

*Total rows returned: 14*

**Insight**: Total revenue across all periods is €445,441.47, with an average monthly revenue of €31,817.25.

---

### 8. How much are courier costs?

**Description**: Calculate total courier costs by time period

**Mart**: `marts.mart_sales_summary`

**Query**:
```sql
SELECT 
    year,
    month,
    month_name,
    ROUND(SUM(courier_base_fee), 2) as total_courier_costs,
    COUNT(DISTINCT purchase_key) as total_purchases,
    ROUND(AVG(courier_base_fee), 2) as avg_courier_fee_per_purchase,
    ROUND(SUM(courier_base_fee) * 100.0 / NULLIF(SUM(total_basket_value), 0), 2) as courier_costs_as_pct_of_basket
FROM marts.mart_sales_summary
GROUP BY year, month, month_name
ORDER BY year, month;
```

**Results** (first 5 rows):

| year | month | month_name | total_courier_costs | total_purchases | avg_courier_fee_per_purchase | courier_costs_as_pct_of_basket |
|---|---|---|---|---|---|---|
| 2022 | 12 | December | 1549.65 | 667 | 1.92 | 35.96 |
| 2023 | 1 | January | 13064.51 | 5333 | 1.89 | 36.01 |
| 2023 | 2 | February | 13956.78 | 5722 | 1.88 | 36.41 |
| 2023 | 3 | March | 18391.22 | 7666 | 1.88 | 36.17 |
| 2023 | 4 | April | 16175.89 | 6685 | 1.88 | 34.78 |

*Total rows returned: 14*

**Insight**: Total courier costs amount to €237,922.68 across all periods, representing an average of 35.76% of basket value.

---

## Notes

- All monetary values are rounded to 2 decimal places
- Queries use `NULLIF()` to handle division by zero safely
- Date filters can be adjusted (e.g., `WHERE order_date >= '2023-01-01'`)
- `LIMIT` clauses can be removed or adjusted based on analysis needs
- For time-based analysis, use `year`, `month`, `quarter`, or `order_date` columns as needed
- All queries have been tested and verified to execute successfully against the marts

**Query Optimization Note**: For demonstration purposes, these queries are presented without additional filters or partitions. In production environments, queries should leverage the available indexes and partitions (as defined in `dbt_project.yml`) for optimal performance. Consider adding appropriate date range filters, using indexed columns in WHERE clauses, and partitioning large tables by time periods to ensure efficient query execution at scale.
