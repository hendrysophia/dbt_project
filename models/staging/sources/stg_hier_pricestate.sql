with
    source as (select * from {{ source("sources", "hier_pricestate") }}),

    renamed as (
        select substate_id as price_substate_id, substate_label, state_id, state_label
        from source
    )

select *
from renamed
