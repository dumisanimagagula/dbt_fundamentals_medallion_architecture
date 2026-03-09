{% docs dim_customers %}

Customer dimension combining user demographics with aggregated order
history. Includes customer classification based on purchase frequency.

{% enddocs %}

{% docs dim_products %}

Product dimension enriched with distribution center details and
inventory statistics.

{% enddocs %}

{% docs dim_distribution_centers %}

Distribution center dimension with product and inventory counts.

{% enddocs %}

{% docs fct_orders %}

Order-level fact table with financial measures aggregated from order
items. One row per order with revenue, cost, profit, and department
breakdowns.

{% enddocs %}

{% docs fct_order_items %}

Line-item level fact table combining order item details with product
pricing. One row per order item with financial measures and fulfillment
timestamps.

{% enddocs %}
