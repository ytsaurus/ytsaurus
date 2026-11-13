-- TPC-DS 4.0.0 query 36; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,item.i_category
   ,item.i_class
   ,grouping(item.i_category)+grouping(item.i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(item.i_category)+grouping(item.i_class),
 	case when grouping(item.i_class) = 0 then item.i_category else null end
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    store_sales
   cross join date_dim       d1
   cross join item
   cross join store
 where
    d1.d_year = 2002
 and d1.d_date_sk = ss_sold_date_sk
 and item.i_item_sk  = ss_item_sk
 and s_store_sk  = ss_store_sk
 and s_state in ('MN', 'TN', 'MI', 'WV', 'NC', 'GA', 'TN', 'OH')
 group by rollup(item.i_category,item.i_class)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then item.i_category else null end
  ,rank_within_parent
  limit 100;
