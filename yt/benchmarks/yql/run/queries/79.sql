-- TPC-DS 4.0.0 query 79; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select
  c_last_name,c_first_name,substring(cast(s_city as string),0,30) bla,ss_ticket_number,amt,profit
  from
   (select store_sales.ss_ticket_number ss_ticket_number
          ,store_sales.ss_customer_sk ss_customer_sk
          ,store.s_city s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales cross join date_dim cross join store cross join household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (household_demographics.hd_dep_count = 0 or household_demographics.hd_vehicle_count > 2)
    and date_dim.d_dow = 1
    and date_dim.d_year in (1999,1999+1,1999+2)
    and store.s_number_employees between 200 and 295
    group by store_sales.ss_ticket_number,store_sales.ss_customer_sk,store_sales.ss_addr_sk,store.s_city) ms cross join customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,bla, profit
limit 100;
