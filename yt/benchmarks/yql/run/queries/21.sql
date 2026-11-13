-- TPC-DS 4.0.0 query 21; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  *
 from(select warehouse.w_warehouse_name w_warehouse_name
            ,item.i_item_id i_item_id
            ,sum(case when (cast(d_date as date) < cast ('2000-05-19' as date))
	                then inv_quantity_on_hand
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('2000-05-19' as date))
                      then inv_quantity_on_hand
                      else 0 end) as inv_after
   from inventory
       cross join warehouse
       cross join item
       cross join date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and cast (d_date as date) between (cast ('2000-05-19' as date) - DateTime::IntervalFromDays(30))
                    and (cast ('2000-05-19' as date) + DateTime::IntervalFromDays(30))
   group by warehouse.w_warehouse_name, item.i_item_id) x
 where (case when inv_before > 0
             then inv_after / inv_before
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 limit 100;
