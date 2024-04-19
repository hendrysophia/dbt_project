with
    source as (select * from {{ source("sources", "hier_clnd") }}),

    renamed as (

        select

            fscldt_id as date_id,
            fscldt_label as date_label,
            fsclwk_id as week_id,
            fsclwk_label as week_label,
            fsclmth_id as month_id,
            fsclmth_label as month_label,
            fsclqrtr_id as qtr_id,
            fsclqrtr_label as qtr_label,
            fsclyr_id as year_id,
            fsclyr_label as year_label,
            ssn_id,
            ssn_label,
            ly_fscldt_id,
            lly_fscldt_id,
            fscldow,
            fscldom,
            fscldoq,
            fscldoy,
            fsclwoy,
            fsclmoy,
            fsclqoy,
            [date] as dt

        from source

    )

select *
from renamed
