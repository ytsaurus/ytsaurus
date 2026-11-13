-- TPC-DS 4.0.0 query 86; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select
    sum(ws_net_paid) as total_sum
   ,i_category
   ,i_class
   ,grouping(item.i_category)+grouping(item.i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(item.i_category)+grouping(item.i_class),
 	case when grouping(item.i_class) = 0 then i_category else null end
 	order by sum(ws_net_paid) desc) as rank_within_parent
 from
    web_sales
    cross join date_dim       d1
   cross join item
 where
    d1.d_month_seq between 1205 and 1205+11
 and d1.d_date_sk = ws_sold_date_sk
 and i_item_sk  = ws_item_sk
 group by rollup(item.i_category,item.i_class)
 order by
   lochierarchy desc,
   case when lochierarchy = 0 then i_category else null end,
   rank_within_parent
 limit 100;
