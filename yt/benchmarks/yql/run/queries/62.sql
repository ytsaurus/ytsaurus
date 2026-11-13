-- TPC-DS 4.0.0 query 62; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select
   bla
  ,ship_mode.sm_type
  ,web_site.web_name
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as `30 days`
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as `31-60 days`
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as `61-90 days`
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as `91-120 days`
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as `>120 days`
from
   web_sales
  cross join warehouse
  cross join ship_mode
  cross join web_site
  cross join date_dim
where
    d_month_seq between 1185 and 1185 + 11
and ws_ship_date_sk   = d_date_sk
and ws_warehouse_sk   = w_warehouse_sk
and ws_ship_mode_sk   = sm_ship_mode_sk
and ws_web_site_sk    = web_site_sk
group by
   substring(cast(w_warehouse_name as string),0,20) as bla
  ,ship_mode.sm_type
  ,web_site.web_name
order by bla
        ,ship_mode.sm_type
       ,web_site.web_name
limit 100;
