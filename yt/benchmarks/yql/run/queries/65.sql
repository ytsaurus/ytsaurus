-- TPC-DS 4.0.0 query 65; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select
	s_store_name,
	i_item_desc,
	sc.revenue,
	i_current_price,
	i_wholesale_cost,
	i_brand
 from store cross join item cross join
     (select ss_store_sk, avg(revenue) as ave
 	from
 	    (select  store_sales.ss_store_sk ss_store_sk, store_sales.ss_item_sk ss_item_sk,
 		     sum(ss_sales_price) as revenue
 		from store_sales cross join date_dim
 		where ss_sold_date_sk = d_date_sk and d_month_seq between 1217 and 1217+11
 		group by store_sales.ss_store_sk, store_sales.ss_item_sk) sa
 	group by ss_store_sk) sb cross join
     (select  store_sales.ss_store_sk ss_store_sk, store_sales.ss_item_sk ss_item_sk, sum(ss_sales_price) as revenue
 	from store_sales cross join date_dim
 	where ss_sold_date_sk = d_date_sk and d_month_seq between 1217 and 1217+11
 	group by store_sales.ss_store_sk, store_sales.ss_item_sk) sc
 where sb.ss_store_sk = sc.ss_store_sk and
       sc.revenue <= 0.1 * sb.ave and
       s_store_sk = sc.ss_store_sk and
       i_item_sk = sc.ss_item_sk
 order by s_store_name, i_item_desc
limit 100;
