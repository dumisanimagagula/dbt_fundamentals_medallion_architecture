with customers as (
    select * from {{ ref('stg_ecommerce__users') }}
),

first_order as (
    select * from {{ ref('int_ecommerce__first_order_created') }}
),

order_summary as (
    select
        user_id,
        count(distinct order_id) as total_orders,
        sum(item_sale_price) as lifetime_revenue,
        sum(item_profit) as lifetime_profit,
        sum(item_discount) as lifetime_discount,
        avg(item_sale_price) as avg_item_sale_price,
        count(order_item_id) as total_items_purchased

    from {{ ref('int_ecommerce__order_items_products') }}
    group by 1
)

select
    cust.user_id,
    cust.first_name,
    cust.last_name,
    cust.email,
    cust.age,
    cust.gender,
    cust.street_address,
    cust.city,
    cust.state,
    cust.postal_code,
    cust.country,
    cust.latitude,
    cust.longitude,
    cust.traffic_source,
    cust.created_at as customer_created_at,
    first_ord.first_order_created_at,
    coalesce(ord_summary.total_orders, 0) as total_orders,
    coalesce(ord_summary.total_items_purchased, 0) as total_items_purchased,
    coalesce(ord_summary.lifetime_revenue, 0) as lifetime_revenue,
    coalesce(ord_summary.lifetime_profit, 0) as lifetime_profit,
    coalesce(ord_summary.lifetime_discount, 0) as lifetime_discount,
    coalesce(ord_summary.avg_item_sale_price, 0) as avg_item_sale_price

from customers as cust
left join first_order as first_ord on cust.user_id = first_ord.user_id
left join order_summary as ord_summary on cust.user_id = ord_summary.user_id
