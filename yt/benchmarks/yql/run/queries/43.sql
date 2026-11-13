-- TPC-DS 4.0.0 query 43; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  store.s_store_name, store.s_store_id,
        sum(case when (d_day_name='Sunday') then ss_sales_price else 0.0 end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else 0.0 end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else 0.0 end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else 0.0 end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else 0.0 end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else 0.0 end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else 0.0 end) sat_sales
 from date_dim cross join store_sales cross join store
 where d_date_sk = ss_sold_date_sk and
       s_store_sk = ss_store_sk and
       s_gmt_offset = -5 and
       d_year = 2000
 group by store.s_store_name, store.s_store_id
 order by store.s_store_name, store.s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
 limit 100;
