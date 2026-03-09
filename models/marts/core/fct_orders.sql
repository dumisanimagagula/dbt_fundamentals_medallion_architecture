{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail',
    partition_by={
      "field": "order_created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["order_status", "user_id"]
) }}

with orders_enriched as (
    select * from {{ ref('int_ecommerce__orders_enriched') }}
)

select
    -- Order identifiers
    ord.order_id,
    ord.user_id,

    -- Order details
    ord.order_status,
    ord.customer_gender,
    ord.num_items_ordered,

    -- Timestamps
    ord.order_created_at,
    ord.order_shipped_at,
    ord.order_delivered_at,
    ord.order_returned_at,

    -- Derived time attributes
    {{ is_weekend('ord.order_created_at') }} as order_created_on_weekend,

    -- Financial measures
    ord.total_sale_price,
    ord.total_product_cost,
    ord.total_profit,
    ord.total_discount,
    ord.total_sold_menswear,
    ord.total_sold_womenswear,

    -- Customer order history context
    ord.days_since_first_order

from orders_enriched as ord
{% if is_incremental() %}
    where ord.order_created_at > (select max(self_ref.order_created_at) from {{ this }} as self_ref)
{% endif %}
