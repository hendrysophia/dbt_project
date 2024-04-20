with
    orders as (select * from {{ ref("stg_hier_clnd") }}),

    prod as (select * from {{ ref("stg_hier_prod") }}),

    transactions as (select * from {{ ref("stg_fact_transactions") }}),

    pricetype as (select * from {{ ref("stg_hier_pricestate") }}),

    possite as (select * from {{ ref("stg_hier_possite") }}),

    weekly_sales_summary as (

        select
        max(t2.dt),
            t1.pos_site_id,
            t1.sku_id,
            t2.week_label,
            t1.price_substate_id,
            type,
            sum(sales_units),
            sum(sales_dollars),
            sum(discount_dollars)
        from transactions as t1
        join orders as t2 on t1.date_id = t2.date_id
        join prod as t3 on t3.sku_id = t1.sku_id
        join pricetype as t4 on t4.price_substate_id = t1.price_substate_id
        join possite as t5 on t5.pos_site_id = t1.pos_site_id
        group by t1.pos_site_id, t1.sku_id, t2.week_label, t1.price_substate_id, type

    )
select *
from weekly_sales_summary
order by week_label
