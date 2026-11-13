-- TPC-DS 4.0.0 query 55; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
select  item.i_brand_id brand_id, item.i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from date_dim cross join store_sales cross join item
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=83
 	and d_moy=12
 	and d_year=2002
 group by item.i_brand, item.i_brand_id
 order by ext_price desc, brand_id
limit 100 ;
