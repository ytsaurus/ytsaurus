-- TPC-DS 4.0.0 query 35; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$bla1 = (select ss_customer_sk customer_sk
          from store_sales cross join date_dim
          where ss_sold_date_sk = d_date_sk and
                d_year = 1999 and
                d_qoy < 4);
$bla2 = (
    (select ws_bill_customer_sk customer_sk
            from web_sales cross join date_dim
            where ws_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_qoy < 4)
    union all
    (select cs_ship_customer_sk customer_sk
            from catalog_sales cross join date_dim
            where cs_sold_date_sk = d_date_sk and
                  d_year = 1999 and
                  d_qoy < 4)
);

select
  ca.ca_state,
  customer_demographics.cd_gender,
  customer_demographics.cd_marital_status,
  customer_demographics.cd_dep_count,
  count(*) cnt1,
  max(customer_demographics.cd_dep_count) aggone1,
  min(customer_demographics.cd_dep_count) aggtwo1,
  max(customer_demographics.cd_dep_count) aggthree1,
  customer_demographics.cd_dep_employed_count,
  count(*) cnt2,
  max(customer_demographics.cd_dep_employed_count) aggone2,
  min(customer_demographics.cd_dep_employed_count) aggtwo2,
  max(cd_dep_employed_count) aggthree2,
  customer_demographics.cd_dep_college_count,
  count(*) cnt3,
  max(customer_demographics.cd_dep_college_count) aggone3,
  min(customer_demographics.cd_dep_college_count) aggtwo3,
  max(customer_demographics.cd_dep_college_count) aggthree3
 from
  customer c cross join customer_address ca cross join customer_demographics
  left semi join $bla1 bla1 on c.c_customer_sk = bla1.customer_sk
  left semi join $bla2 bla2 on c.c_customer_sk = bla2.customer_sk
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk
group by ca.ca_state,
          customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_dep_count,
          customer_demographics.cd_dep_employed_count,
          customer_demographics.cd_dep_college_count
 order by ca.ca_state,
          customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_dep_count,
          customer_demographics.cd_dep_employed_count,
          customer_demographics.cd_dep_college_count
 limit 100;
