with
    source as (select * from {{ source("sources", "fact_averagecosts") }}),

    renamed as (

        select
            {{ dbt_utils.surrogate_key(["fscldt_id", "sku_id"]) }}
            as averagecost_item_key,
            fscldt_id as date_id,
            sku_id,
            average_unit_standardcost,
            average_unit_landedcost
        from source

    )

select *
from renamed
