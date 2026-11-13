-- TPC-DS 4.0.0 query 27; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  item.i_item_id,
        store.s_state, grouping(store.s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales cross join customer_demographics cross join date_dim cross join store cross join item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'S' and
       cd_education_status = 'Advanced Degree' and
       d_year = 1999 and
       s_state in ('TX', 'MI', 'IL', 'MN', 'NM', 'SD')
 group by rollup (item.i_item_id, store.s_state)
 order by item.i_item_id
         ,store.s_state
 limit 100;
