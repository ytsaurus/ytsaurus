-- TPC-DS 4.0.0 query 46; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,amt,profit
 from
   (select store_sales.ss_ticket_number ss_ticket_number
          ,store_sales.ss_customer_sk ss_customer_sk
          ,customer_address.ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales cross join date_dim cross join store cross join household_demographics cross join customer_address
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and (household_demographics.hd_dep_count = 9 or
         household_demographics.hd_vehicle_count= 1)
    and date_dim.d_dow in (6,0)
    and date_dim.d_year in (1998,1998+1,1998+2)
    and store.s_city in ('New Hope', 'Hamilton', 'Georgetown', 'Hillcrest', 'Walnut Grove')
    group by store_sales.ss_ticket_number,store_sales.ss_customer_sk,store_sales.ss_addr_sk,customer_address.ca_city) dn cross join customer cross join customer_address current_addr
    where ss_customer_sk = c_customer_sk
      and customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number
  limit 100;
