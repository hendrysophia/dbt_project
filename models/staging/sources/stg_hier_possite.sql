with
    source as (select * from {{ source("sources", "hier_possite") }}),

    renamed as (
        select
            site_id as pos_site_id,
            site_label,
            subchnl_id,
            subchnl_label,
            chnl_id,
            chnl_label
        from source
    )

select *
from renamed
