{{ config(
    partition_by={
      "field": "order_item_created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["order_item_status", "product_department"]
) }}

with order_items_products as (
    select * from {{ ref('int_ecommerce__order_items_products') }}
)

select
    -- Identifiers
    oip.order_item_id,
    oip.order_id,
    oip.user_id,
    oip.product_id,

    -- Status & timestamps
    oip.status as order_item_status,
    oip.created_at as order_item_created_at,
    oip.shipped_at,
    oip.delivered_at,
    oip.returned_at,

    -- Financial measures
    oip.item_sale_price,
    oip.product_cost,
    oip.product_retail_price,
    oip.item_profit,
    oip.item_discount,

    -- Product attributes
    oip.product_department

from order_items_products as oip
