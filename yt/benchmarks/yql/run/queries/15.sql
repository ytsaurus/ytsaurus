-- TPC-DS 4.0.0 query 15; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries

select  customer_address.ca_zip
       ,sum(cs_sales_price)
 from catalog_sales
     cross join customer
     cross join customer_address
     cross join date_dim
 where cs_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk
 	and ( substring(cast(ca_zip as string),0,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
 	      or cs_sales_price > 500)
 	and cs_sold_date_sk = d_date_sk
 	and d_qoy = 1 and d_year = 2002
 group by customer_address.ca_zip
 order by customer_address.ca_zip
 limit 100;
