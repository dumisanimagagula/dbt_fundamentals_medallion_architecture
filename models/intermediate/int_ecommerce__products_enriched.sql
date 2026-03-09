with products as (
    select * from {{ ref('stg_ecommerce__products') }}
),

distribution_centers as (
    select * from {{ ref('stg_ecommerce__distribution_centers') }}
),

inventory_stats as (
    select
        product_id,
        count(*) as total_inventory_items,
        count(sold_at) as total_sold_items,
        count(*) - count(sold_at) as items_in_stock,
        avg(cost) as avg_inventory_cost

    from {{ ref('stg_ecommerce__inventory_items') }}
    group by 1
)

select
    prod.product_id,
    prod.sku as product_sku,
    prod.product_name,
    prod.category as product_category,
    prod.brand as product_brand,
    prod.department as product_department,
    prod.cost as product_cost,
    prod.retail_price as product_retail_price,
    prod.distribution_center_id,
    dist_ctr.distribution_center_name,
    dist_ctr.latitude as distribution_center_latitude,
    dist_ctr.longitude as distribution_center_longitude,
    prod.retail_price - prod.cost as product_margin,
    coalesce(inv.total_inventory_items, 0) as total_inventory_items,
    coalesce(inv.total_sold_items, 0) as total_sold_items,
    coalesce(inv.items_in_stock, 0) as items_in_stock

from products as prod
left join distribution_centers as dist_ctr
    on prod.distribution_center_id = dist_ctr.distribution_center_id
left join inventory_stats as inv
    on prod.product_id = inv.product_id
