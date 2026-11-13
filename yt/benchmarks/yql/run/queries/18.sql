-- TPC-DS 4.0.0 query 18; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  item.i_item_id,
        customer_address.ca_country,
        customer_address.ca_state,
        customer_address.ca_county,
        avg( cast(cs_quantity as float)) agg1,
        avg( cast(cs_list_price as float)) agg2,
        avg( cast(cs_coupon_amt as float)) agg3,
        avg( cast(cs_sales_price as float)) agg4,
        avg( cast(cs_net_profit as float)) agg5,
        avg( cast(c_birth_year as float)) agg6,
        avg( cast(cd1.cd_dep_count as float)) agg7
 from catalog_sales
 cross join customer_demographics cd1
 cross join customer_demographics cd2
 cross join customer
 cross join customer_address
 cross join date_dim
 cross join item
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd1.cd_demo_sk and
       cs_bill_customer_sk = c_customer_sk and
       cd1.cd_gender = 'M' and
       cd1.cd_education_status = 'Primary' and
       c_current_cdemo_sk = cd2.cd_demo_sk and
       c_current_addr_sk = ca_address_sk and
       c_birth_month in (9, 5, 1, 2, 3, 8) and
       d_year = 1999 and
       ca_state in ('NV', 'AL', 'CA', 'WI', 'SD', 'OK', 'KY')
 group by rollup (item.i_item_id, customer_address.ca_country, customer_address.ca_state, customer_address.ca_county)
 order by customer_address.ca_country,
        customer_address.ca_state,
        customer_address.ca_county,
	item.i_item_id
 limit 100;
