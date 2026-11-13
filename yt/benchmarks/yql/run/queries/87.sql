-- TPC-DS 4.0.0 query 87; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$bla1 = (select distinct c_last_name, c_first_name, d_date
       from store_sales cross join date_dim cross join  customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1217 and 1217+11);

$bla2 = ((select distinct c_last_name, c_first_name, d_date
       from catalog_sales cross join date_dim cross join customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1217 and 1217+11)
       union all
      (select distinct c_last_name, c_first_name, d_date
       from web_sales cross join date_dim cross join customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1217 and 1217+11));

select count(*)
from $bla1 bla1 left semi join $bla2 bla2 using (c_last_name, c_first_name, d_date)
;
