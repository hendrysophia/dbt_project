with
    orders as (select * from {{ ref("stg_hier_clnd") }}),

    transactions as (select * from {{ ref("stg_fact_transactions") }}),

    weekly_sales_summary as (

        select
            pos_site_id,
            sku_id,
            t2.week_id,
            price_substate_id,
            type,
            sum(sales_units),
            sum(sales_dollars),
            sum(discount_dollars)
        from transactions t1
        join orders as t2 on t1.date_id = t2.date_id
        group by pos_site_id, sku_id, t2.week_id, price_substate_id, type

    )
select *
from weekly_sales_summary
order by week_id