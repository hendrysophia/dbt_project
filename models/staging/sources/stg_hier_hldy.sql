with
    source as (select * from {{ source("sources", "hier_hldy") }}),

    renamed as (select hldy_id as date_id, hldy_label as date_label from source)

select *
from renamed
