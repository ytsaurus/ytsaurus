-- TPC-DS 4.0.0 query 20; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  item.i_item_id
       ,item.i_item_desc
       ,item.i_category
       ,item.i_class
       ,item.i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by item.i_class) as revenueratio
 from	catalog_sales
     cross join item
     cross join date_dim
 where cs_item_sk = i_item_sk
   and i_category in ('Shoes', 'Sports', 'Electronics')
   and cs_sold_date_sk = d_date_sk
 and cast(d_date as date) between cast('1998-05-13' as date)
 				and (cast('1998-05-13' as date) + DateTime::IntervalFromDays(30))
 group by item.i_item_id
         ,item.i_item_desc
         ,item.i_category
         ,item.i_class
         ,item.i_current_price
 order by item.i_category
         ,item.i_class
         ,item.i_item_id
         ,item.i_item_desc
         ,revenueratio
limit 100;
