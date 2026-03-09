with distribution_centers_enriched as (
    select * from {{ ref('int_ecommerce__distribution_centers_enriched') }}
)

select
    dc.distribution_center_id,
    dc.distribution_center_name,
    dc.latitude,
    dc.longitude,

    -- Product & inventory metrics
    dc.total_products,
    dc.total_inventory_items,
    dc.total_sold_items

from distribution_centers_enriched as dc
