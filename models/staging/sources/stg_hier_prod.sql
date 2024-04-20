with
    source as (select * from {{ source("sources", "hier_prod") }}),

    renamed as (
        select
            sku_id,
            sku_label,
            stylclr_id,
            stylclr_label,
            styl_id,
            styl_label,
            subcat_id,
            subcat_label,
            cat_id,
            cat_label,
            dept_id,
            dept_label,
            issvc,
            isasmbly,
            isnfs
        from source
    )

select *
from renamed
