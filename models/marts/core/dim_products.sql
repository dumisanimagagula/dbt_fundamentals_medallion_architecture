with products_enriched as (
    select * from {{ ref('int_ecommerce__products_enriched') }}
)

select
    -- Product identifiers
    pe.product_id,
    pe.product_sku,

    -- Product details
    pe.product_name,
    pe.product_category,
    pe.product_brand,
    pe.product_department,

    -- Pricing
    pe.product_cost,
    pe.product_retail_price,
    pe.product_margin,

    -- Distribution center info
    pe.distribution_center_id,
    pe.distribution_center_name,
    pe.distribution_center_latitude,
    pe.distribution_center_longitude,

    -- Inventory stats
    pe.total_inventory_items,
    pe.total_sold_items,
    pe.items_in_stock

from products_enriched as pe
