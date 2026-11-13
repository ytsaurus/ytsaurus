-- TPC-DS 4.0.0 query 48; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select sum (ss_quantity)
 from store_sales cross join store cross join customer_demographics cross join customer_address cross join date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 1999
 and
 (
  (
   cd_demo_sk = ss_cdemo_sk
   and
   cd_marital_status = 'D'
   and
   cd_education_status = 'Secondary'
   and
   ss_sales_price between 100.00 and 150.00
   )
 or
  (
  cd_demo_sk = ss_cdemo_sk
   and
   cd_marital_status = 'M'
   and
   cd_education_status = '2 yr Degree'
   and
   ss_sales_price between 50.00 and 100.00
  )
 or
 (
  cd_demo_sk = ss_cdemo_sk
  and
   cd_marital_status = 'W'
   and
   cd_education_status = '4 yr Degree'
   and
   ss_sales_price between 150.00 and 200.00
 )
 )
 and
 (
  (
  ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('IN', 'WV', 'VA')
  and ss_net_profit between 0 and 2000
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('TX', 'ND', 'MN')
  and ss_net_profit between 150 and 3000
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('SD', 'GA', 'CO')
  and ss_net_profit between 50 and 25000
  )
 )
;
