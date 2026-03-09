with distribution_centers as (
    select * from {{ ref('stg_ecommerce__distribution_centers') }}
),

product_counts as (
    select
        distribution_center_id,
        count(*) as total_products

    from {{ ref('stg_ecommerce__products') }}
    group by 1
),

inventory_counts as (
    select
        product_distribution_center_id as distribution_center_id,
        count(*) as total_inventory_items,
        count(sold_at) as total_sold_items

    from {{ ref('stg_ecommerce__inventory_items') }}
    group by 1
)

select
    dist_ctr.distribution_center_id,
    dist_ctr.distribution_center_name,
    dist_ctr.latitude,
    dist_ctr.longitude,
    coalesce(prod_cnt.total_products, 0) as total_products,
    coalesce(inv_cnt.total_inventory_items, 0) as total_inventory_items,
    coalesce(inv_cnt.total_sold_items, 0) as total_sold_items

from distribution_centers as dist_ctr
left join product_counts as prod_cnt
    on dist_ctr.distribution_center_id = prod_cnt.distribution_center_id
left join inventory_counts as inv_cnt
    on dist_ctr.distribution_center_id = inv_cnt.distribution_center_id
