-- TPC-DS 4.0.0 query 90; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from web_sales cross join household_demographics cross join time_dim cross join web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 7 and 7+1
         and household_demographics.hd_dep_count = 9
         and web_page.wp_char_count between 5000 and 5200) at cross join
      ( select count(*) pmc
       from web_sales cross join household_demographics cross join time_dim cross join web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 18 and 18+1
         and household_demographics.hd_dep_count = 9
         and web_page.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 limit 100;
