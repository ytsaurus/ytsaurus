-- TPC-DS 4.0.0 query 16; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$orders_with_several_warehouses = (
    select cs_order_number
    from catalog_sales
    group by cs_order_number
    having count(distinct cs_warehouse_sk) > 1
);

select
   count(distinct cs1.cs_order_number) as `order count`
  ,sum(cs_ext_ship_cost) as `total shipping cost`
  ,sum(cs_net_profit) as `total net profit`
from
   catalog_sales cs1
  cross join date_dim
  cross join customer_address
  cross join call_center
  left semi join $orders_with_several_warehouses cs2 on cs1.cs_order_number = cs2.cs_order_number
  left only join catalog_returns cr1 on cs1.cs_order_number = cr1.cr_order_number
where
    cast(d_date as date) between cast('2000-02-01' as date) and
           (cast('2000-02-01' as date) + DateTime::IntervalFromDays(60))
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'AL'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Dauphin County', 'Levy County', 'Luce County', 'Jackson County', 'Daviess County')
order by `order count`
limit 100;
