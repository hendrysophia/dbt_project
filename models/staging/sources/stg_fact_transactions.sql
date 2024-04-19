with
    source as (select * from {{ source("sources", "fact_transactions") }}),

    renamed as (

        select
            {{ dbt_utils.surrogate_key(["ORDER_ID", "LINE_ID", "SKU_ID"]) }}
            as transaction_item_key,
            order_id,
            line_id,
            type,
            dt,
            pos_site_id,
            sku_id,
            fscldt_id as date_id,
            price_substate_id,
            sales_units,
            sales_dollars,
            discount_dollars,
            original_order_id,
            original_line_id
        from source

    )

select *
from renamed