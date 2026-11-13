-- TPC-DS 4.0.0 query 69; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$bla1 = (select ss_customer_sk customer_sk
          from store_sales cross join date_dim
          where ss_sold_date_sk = d_date_sk and
                d_year = 2002 and
                d_moy between 1 and 1+2);

$bla2 = ((select ws_bill_customer_sk customer_sk
            from web_sales cross join date_dim
            where
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2002 and
                  d_moy between 1 and 1+2)
        union all
        (select cs_ship_customer_sk customer_sk
            from catalog_sales cross join date_dim
            where
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2002 and
                  d_moy between 1 and 1+2)
        );

select
  customer_demographics.cd_gender,
  customer_demographics.cd_marital_status,
  customer_demographics.cd_education_status,
  count(*) cnt1,
  customer_demographics.cd_purchase_estimate,
  count(*) cnt2,
  customer_demographics.cd_credit_rating,
  count(*) cnt3
 from
  customer c cross join customer_address ca cross join customer_demographics
  left semi join $bla1 bla1 on c.c_customer_sk = bla1.customer_sk
  left only join $bla2 bla2 on c.c_customer_sk = bla2.customer_sk
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('CO','SD','TX') and
  cd_demo_sk = c.c_current_cdemo_sk
 group by customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_education_status,
          customer_demographics.cd_purchase_estimate,
          customer_demographics.cd_credit_rating
 order by customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_education_status,
          customer_demographics.cd_purchase_estimate,
          customer_demographics.cd_credit_rating
 limit 100;
