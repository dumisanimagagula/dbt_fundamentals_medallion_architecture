with orders as (
    select * from {{ ref('stg_ecommerce__orders') }}
),

order_item_measures as (
    select
        order_id,
        sum(item_sale_price) as total_sale_price,
        sum(product_cost) as total_product_cost,
        sum(item_profit) as total_profit,
        sum(item_discount) as total_discount,
        sum(if(product_department = 'Men', item_sale_price, 0)) as total_sold_menswear,
        sum(if(product_department = 'Women', item_sale_price, 0)) as total_sold_womenswear

    from {{ ref('int_ecommerce__order_items_products') }}
    group by 1
),

first_order as (
    select * from {{ ref('int_ecommerce__first_order_created') }}
)

select
    ord.order_id,
    ord.user_id,
    ord.status as order_status,
    ord.gender as customer_gender,
    ord.num_items_ordered,
    ord.created_at as order_created_at,
    ord.shipped_at as order_shipped_at,
    ord.delivered_at as order_delivered_at,
    ord.returned_at as order_returned_at,
    oim.total_sale_price,
    oim.total_product_cost,
    oim.total_profit,
    oim.total_discount,
    oim.total_sold_menswear,
    oim.total_sold_womenswear,
    first_ord.first_order_created_at,
    timestamp_diff(ord.created_at, first_ord.first_order_created_at, day) as days_since_first_order

from orders as ord
left join order_item_measures as oim
    on ord.order_id = oim.order_id
left join first_order as first_ord
    on ord.user_id = first_ord.user_id
