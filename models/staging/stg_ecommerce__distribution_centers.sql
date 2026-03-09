with source as (
    select *

    from {{ source('thelook_ecommerce', 'distribution_centers') }}
)

select
    -- IDs
    id as distribution_center_id,

    -- Details
    name as distribution_center_name,
    latitude,
    longitude

from source
