{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='fail',
    partition_by={
      "field": "event_created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["event_type", "user_id"]
) }}

with events_enriched as (
    select * from {{ ref('int_ecommerce__events_enriched') }}
    {% if is_incremental() %}
        where event_created_at > (select max(self_ref.event_created_at) from {{ this }} as self_ref)
    {% endif %}
)

select
    -- Event identifiers
    evt.event_id,
    evt.user_id,
    evt.session_id,
    evt.sequence_number,

    -- Event details
    evt.event_type,
    evt.browser,
    evt.event_traffic_source,
    evt.uri,
    evt.ip_address,

    -- Event location
    evt.event_city,
    evt.event_state,
    evt.event_postal_code,

    -- Timestamps
    evt.event_created_at,

    -- User context
    evt.user_traffic_source,
    evt.user_country,

    -- Derived flags
    coalesce(evt.event_type = 'purchase', false) as is_purchase_event

from events_enriched as evt
