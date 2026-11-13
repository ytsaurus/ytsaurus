-- TPC-DS 4.0.0 query 6; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
$sub1 = (select distinct (d_month_seq)
 	      from date_dim
               where d_year = 2000
 	        and d_moy = 6 );

$sub2 = (select AVG(j.i_current_price) as i_current_price, j.i_category as i_category
 	            from item as j
                group by j.i_category);


select  a.ca_state state, count(*) cnt
 from customer_address a
     cross join customer c
     cross join store_sales s
     cross join date_dim d
     cross join item i
     left join $sub2 as j on i.i_category = j.i_category
 where       a.ca_address_sk = c.c_current_addr_sk
 	and c.c_customer_sk = s.ss_customer_sk
 	and s.ss_sold_date_sk = d.d_date_sk
 	and s.ss_item_sk = i.i_item_sk
 	and d.d_month_seq = $sub1

 	and i.i_current_price > 1.2 * j.i_current_price
 group by a.ca_state
 having count(*) >= 10
 order by cnt, state
 limit 100;
