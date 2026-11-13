-- TPC-DS 4.0.0 query 57; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$v1 = (
 select item.i_category i_category, item.i_brand i_brand,
        call_center.cc_name cc_name,
        date_dim.d_year d_year, date_dim.d_moy d_moy,
        sum(cs_sales_price) sum_sales,
        avg(sum(cs_sales_price)) over
          (partition by item.i_category, item.i_brand,
                     call_center.cc_name, date_dim.d_year)
          avg_monthly_sales,
        rank() over
          (partition by item.i_category, item.i_brand,
                     call_center.cc_name
           order by date_dim.d_year, date_dim.d_moy) rn
 from item cross join catalog_sales cross join  date_dim cross join call_center
 where cs_item_sk = i_item_sk and
       cs_sold_date_sk = d_date_sk and
       cc_call_center_sk= cs_call_center_sk and
       (
         d_year = 2001 or
         ( d_year = 2001-1 and d_moy =12) or
         ( d_year = 2001+1 and d_moy =1)
       )
 group by item.i_category, item.i_brand,
          call_center.cc_name , date_dim.d_year, date_dim.d_moy);
$v2 = (
 select v1.cc_name cc_name
        ,v1.d_year d_year, v1.d_moy d_moy
        ,v1.avg_monthly_sales avg_monthly_sales
        ,v1.sum_sales sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from $v1 v1 cross join $v1 v1_lag cross join $v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1. cc_name = v1_lag. cc_name and
       v1. cc_name = v1_lead. cc_name and
       v1.rn = v1_lag.rn + 1 and
       v1.rn = v1_lead.rn - 1);
  select  *
 from $v2
 where  d_year = 2001 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
 order by sum_sales - avg_monthly_sales, psum
 limit 100;
