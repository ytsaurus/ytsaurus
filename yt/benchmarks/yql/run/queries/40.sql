-- TPC-DS 4.0.0 query 40; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select
   warehouse.w_state
  ,item.i_item_id
  ,sum(case when (cast(d_date as date) < cast ('2001-03-21' as date))
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('2001-03-21' as date))
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
 from
   catalog_sales left join catalog_returns on
       (catalog_sales.cs_order_number = catalog_returns.cr_order_number
        and catalog_sales.cs_item_sk = catalog_returns.cr_item_sk)
  cross join warehouse
  cross join item
  cross join date_dim
 where
     i_current_price between 0.99 and 1.49
 and i_item_sk          = cs_item_sk
 and cs_warehouse_sk    = w_warehouse_sk
 and cs_sold_date_sk    = d_date_sk
 and cast(d_date as date) between (cast ('2001-03-21' as date) - DateTime::IntervalFromDays(30))
                and (cast ('2001-03-21' as date) + DateTime::IntervalFromDays(30))
 group by
    warehouse.w_state,item.i_item_id
 order by warehouse.w_state,item.i_item_id
limit 100;
