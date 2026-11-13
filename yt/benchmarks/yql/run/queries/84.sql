-- TPC-DS 4.0.0 query 84; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from customer
    cross join customer_address
     cross join customer_demographics
     cross join household_demographics
     cross join income_band
     cross join store_returns
 where ca_city	        =  'Greenville'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  36218
   and ib_upper_bound   <=  36218 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by customer_id
 limit 100;
