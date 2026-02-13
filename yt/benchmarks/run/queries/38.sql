-- NB: Subqueries
$bla1 = (select distinct c_last_name, c_first_name, d_date
    from store_sales cross join date_dim cross join customer
          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
      and store_sales.ss_customer_sk = customer.c_customer_sk
      and d_month_seq between 1200 and 1200 + 11);

$bla2 = (select distinct c_last_name, c_first_name, d_date
    from catalog_sales cross join date_dim cross join customer
          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1200 and 1200 + 11);

$bla3 = (select distinct c_last_name, c_first_name, d_date
    from web_sales cross join date_dim cross join customer
          where web_sales.ws_sold_date_sk = date_dim.d_date_sk
      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1200 and 1200 + 11);

-- start query 1 in stream 0 using template query38.tpl and seed 1819994127
select count(*) from (
    select
        bla1.c_last_name as c_last_name,
        bla1.c_first_name as c_first_name,
        bla1.d_date as d_date
    from any $bla1 bla1
    join any $bla2 bla2 on StablePickle(bla1.c_last_name) = StablePickle(bla2.c_last_name) and StablePickle(bla1.c_first_name) = StablePickle(bla2.c_first_name) and StablePickle(bla1.d_date) = StablePickle(bla2.d_date)
    join any $bla3 bla3 on StablePickle(bla1.c_last_name) = StablePickle(bla3.c_last_name) and StablePickle(bla1.c_first_name) = StablePickle(bla3.c_first_name) and StablePickle(bla1.d_date) = StablePickle(bla3.d_date)
) hot_cust
limit 100;

-- end query 1 in stream 0 using template query38.tpl
