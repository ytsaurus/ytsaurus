-- TPC-DS 4.0.0 query 37; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  item.i_item_id
       ,item.i_item_desc
       ,item.i_current_price
 from item cross join inventory cross join date_dim cross join catalog_sales
 where i_current_price between 24 and 24 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and cast(d_date as date) between cast('2002-06-22' as date) and (cast('2002-06-22' as date) + DateTime::IntervalFromDays(60))
 and i_manufact_id in (951,717,692,953)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by item.i_item_id,item.i_item_desc,item.i_current_price
 order by item.i_item_id
 limit 100;
