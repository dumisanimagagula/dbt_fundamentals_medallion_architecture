with source as (
    select *

    from {{ source('thelook_ecommerce', 'order_items') }}
)

select
    -- IDs
    id as order_item_id,
    order_id,
    user_id,
    product_id,

    -- Order item details
    status,
    sale_price as item_sale_price,

    -- Timestamps
    created_at,
    shipped_at,
    delivered_at,
    returned_at

from source
