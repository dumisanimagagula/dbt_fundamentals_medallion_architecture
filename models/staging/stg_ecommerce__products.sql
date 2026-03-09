with source as (
    select *

    from {{ source('thelook_ecommerce', 'products') }}
)

select
    -- IDs
    id as product_id,
    distribution_center_id,

    -- Product details
    name as product_name,
    category,
    brand,
    sku,
    department,

    -- Pricing
    cost,
    retail_price

from source
