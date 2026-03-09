with source as (

    select * from {{ source('thelook_ecommerce', 'events') }}

),

renamed as (

    select
        -- ids
        id as event_id,
        user_id,
        sequence_number,
        session_id,

        -- event details
        event_type,
        browser,
        traffic_source,
        uri,
        ip_address,

        -- location
        city,
        state,
        postal_code,

        -- timestamps
        created_at

    from source

)

select * from renamed
