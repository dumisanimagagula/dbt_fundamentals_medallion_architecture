with events as (
    select * from {{ ref('stg_ecommerce__events') }}
),

users as (
    select
        user_id,
        traffic_source as user_traffic_source,
        country as user_country

    from {{ ref('stg_ecommerce__users') }}
)

select
    evt.event_id,
    evt.user_id,
    evt.session_id,
    evt.sequence_number,
    evt.event_type,
    evt.browser,
    evt.traffic_source as event_traffic_source,
    evt.uri,
    evt.ip_address,
    evt.city as event_city,
    evt.state as event_state,
    evt.postal_code as event_postal_code,
    evt.created_at as event_created_at,
    usr.user_traffic_source,
    usr.user_country

from events as evt
left join users as usr
    on evt.user_id = usr.user_id
