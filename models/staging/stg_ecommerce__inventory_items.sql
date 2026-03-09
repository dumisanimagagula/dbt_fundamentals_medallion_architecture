with source as (

    select * from {{ source('thelook_ecommerce', 'inventory_items') }}

),

renamed as (

    select
        -- ids
        id as inventory_item_id,
        product_id,
        product_distribution_center_id,

        -- product details
        product_name,
        product_category,
        product_brand,
        product_department,
        product_sku,

        -- pricing
        cost,
        product_retail_price,

        -- timestamps
        created_at,
        sold_at

    from source

)

select * from renamed
