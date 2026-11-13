-- TPC-DS 4.0.0 query 33; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$ss = (
 select
          item.i_manufact_id i_manufact_id,sum(ss_ext_sales_price) total_sales
 from
 	store_sales cross join
 	date_dim cross join
         customer_address cross join
         item
 where
         item.i_manufact_id in (select
  i_manufact_id
from
 item
where i_category in ('Jewelry'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 3
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -5
 group by item.i_manufact_id);

$cs = (
 select
          item.i_manufact_id i_manufact_id,sum(cs_ext_sales_price) total_sales
 from
 	catalog_sales cross join
 	date_dim cross join
         customer_address cross join
         item
 where
         item.i_manufact_id               in (select
  i_manufact_id
from
 item
where i_category in ('Jewelry'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 3
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5
 group by item.i_manufact_id);

$ws = (
 select
          item.i_manufact_id i_manufact_id,sum(ws_ext_sales_price) total_sales
 from
 	web_sales cross join
 	date_dim cross join
         customer_address cross join
         item
 where
         item.i_manufact_id               in (select
  i_manufact_id
from
 item
where i_category in ('Jewelry'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 2001
 and     d_moy                   = 3
 and     ws_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5
 group by item.i_manufact_id);


select  i_manufact_id ,sum(total_sales) total_sales
 from  (select * from $ss
        union all
        select * from $cs
        union all
        select * from $ws) tmp1
 group by i_manufact_id
 order by total_sales
limit 100;
