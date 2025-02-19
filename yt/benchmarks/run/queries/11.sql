-- NB: Subqueries

-- start query 1 in stream 0 using template query11.tpl and seed 1819994127
$year_total = (
 select customer.c_customer_id customer_id
       ,customer.c_first_name customer_first_name
       ,customer.c_last_name customer_last_name
       ,customer.c_preferred_cust_flag customer_preferred_cust_flag
       ,customer.c_birth_country customer_birth_country
       ,customer.c_login customer_login
       ,customer.c_email_address customer_email_address
       ,date_dim.d_year dyear
       ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total
       ,'s' sale_type
 from customer
     cross join store_sales
     cross join date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
 group by customer.c_customer_id
         ,customer.c_first_name
         ,customer.c_last_name
         ,customer.c_preferred_cust_flag
         ,customer.c_birth_country
         ,customer.c_login
         ,customer.c_email_address
         ,date_dim.d_year
 union all
 select customer.c_customer_id customer_id
       ,customer.c_first_name customer_first_name
       ,customer.c_last_name customer_last_name
       ,customer.c_preferred_cust_flag customer_preferred_cust_flag
       ,customer.c_birth_country customer_birth_country
       ,customer.c_login customer_login
       ,customer.c_email_address customer_email_address
       ,date_dim.d_year dyear
       ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total
       ,'w' sale_type
 from customer
     cross join web_sales
     cross join date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
 group by customer.c_customer_id
         ,customer.c_first_name
         ,customer.c_last_name
         ,customer.c_preferred_cust_flag
         ,customer.c_birth_country
         ,customer.c_login
         ,customer.c_email_address
         ,date_dim.d_year
         );

select
 t_s_secyear.customer_id
 ,t_s_secyear.customer_first_name
 ,t_s_secyear.customer_last_name
 ,t_s_secyear.customer_birth_country
 from $year_total t_s_firstyear
     cross join $year_total t_s_secyear
     cross join $year_total t_w_firstyear
     cross join $year_total t_w_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
         and t_s_firstyear.customer_id = t_w_secyear.customer_id
         and t_s_firstyear.customer_id = t_w_firstyear.customer_id
         and t_s_firstyear.sale_type = 's'
         and t_w_firstyear.sale_type = 'w'
         and t_s_secyear.sale_type = 's'
         and t_w_secyear.sale_type = 'w'
         and t_s_firstyear.dyear = 1999
         and t_s_secyear.dyear = 1999+1
         and t_w_firstyear.dyear = 1999
         and t_w_secyear.dyear = 1999+1
         and t_s_firstyear.year_total > 0
         and t_w_firstyear.year_total > 0
         and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else 0.0 end
             > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else 0.0 end
 order by t_s_secyear.customer_id
         ,t_s_secyear.customer_first_name
         ,t_s_secyear.customer_last_name
         ,t_s_secyear.customer_birth_country
limit 100;

-- end query 1 in stream 0 using template query11.tpl
