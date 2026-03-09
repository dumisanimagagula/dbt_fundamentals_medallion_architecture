with source as (

    select * from {{ source('thelook_ecommerce', 'users') }}

),

renamed as (

    select
        -- ids
        id as user_id,

        -- personal details
        first_name,
        last_name,
        email,
        age,
        gender,

        -- location
        street_address,
        city,
        state,
        postal_code,
        country,
        latitude,
        longitude,

        -- acquisition
        traffic_source,

        -- timestamps
        created_at

    from source

)

select * from renamed
