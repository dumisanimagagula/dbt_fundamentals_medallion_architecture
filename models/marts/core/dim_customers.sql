with customers_enriched as (
    select * from {{ ref('int_ecommerce__customers_enriched') }}
)

select
    -- Customer identifiers
    cust.user_id as customer_id,
    cust.first_name,
    cust.last_name,
    cust.email,

    -- Demographics
    cust.age,
    cust.gender,

    -- Location
    cust.street_address,
    cust.city,
    cust.state,
    cust.postal_code,
    cust.country,
    cust.latitude,
    cust.longitude,

    -- Acquisition
    cust.traffic_source,
    cust.customer_created_at,

    -- Order history
    cust.first_order_created_at,
    cust.total_orders,
    cust.total_items_purchased,
    cust.lifetime_revenue,
    cust.lifetime_profit,
    cust.lifetime_discount,
    cust.avg_item_sale_price,

    -- Customer classification
    case
        when cust.total_orders = 0 then 'never_purchased'
        when cust.total_orders = 1 then 'single_purchase'
        when cust.total_orders between 2 and 3 then 'repeat_buyer'
        else 'loyal_customer'
    end as customer_type

from customers_enriched as cust
