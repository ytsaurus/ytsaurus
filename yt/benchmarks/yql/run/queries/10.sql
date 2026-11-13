-- TPC-DS 4.0.0 query 10; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$bla1 = (select DISTINCT ss_customer_sk
          from store_sales cross join date_dim
          where ss_sold_date_sk = d_date_sk and
                d_year = 2000 and
                d_moy between 2 and 2+3);
$bla2 = (select DISTINCT customer_sk from ((select ws_bill_customer_sk customer_sk
            from web_sales cross join date_dim
            where ws_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_moy between 2 AND 2+3)
        union all
        (select cs_ship_customer_sk customer_sk
            from catalog_sales cross join date_dim
            where
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_moy between 2 and 2+3)));

select
  customer_demographics.cd_gender,
  customer_demographics.cd_marital_status,
  customer_demographics.cd_education_status,
  count(*) cnt1,
  customer_demographics.cd_purchase_estimate,
  count(*) cnt2,
  customer_demographics.cd_credit_rating,
  count(*) cnt3,
  customer_demographics.cd_dep_count,
  count(*) cnt4,
  customer_demographics.cd_dep_employed_count,
  count(*) cnt5,
  customer_demographics.cd_dep_college_count,
  count(*) cnt6
 from
  customer c cross join customer_address ca cross join customer_demographics
  left semi join $bla1 bla1 on (c.c_customer_sk = bla1.ss_customer_sk)
  left semi join $bla2 bla2 on (c.c_customer_sk = bla2.customer_sk)
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('Chautauqua County','Cherokee County','Bell County','Washington County','Clarke County') and
  cd_demo_sk = c.c_current_cdemo_sk
 group by customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_education_status,
          customer_demographics.cd_purchase_estimate,
          customer_demographics.cd_credit_rating,
          customer_demographics.cd_dep_count,
          customer_demographics.cd_dep_employed_count,
          customer_demographics.cd_dep_college_count
 order by customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_education_status,
          customer_demographics.cd_purchase_estimate,
          customer_demographics.cd_credit_rating,
          customer_demographics.cd_dep_count,
          customer_demographics.cd_dep_employed_count,
          customer_demographics.cd_dep_college_count
limit 100;
