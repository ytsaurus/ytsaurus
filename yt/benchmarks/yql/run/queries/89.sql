-- TPC-DS 4.0.0 query 89; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  *
from(
select item.i_category, item.i_class, item.i_brand,
       store.s_store_name s_store_name, store.s_company_name,
       date_dim.d_moy,
       sum(ss_sales_price) sum_sales,
       avg(sum(ss_sales_price)) over
         (partition by item.i_category, item.i_brand, store.s_store_name, store.s_company_name)
         avg_monthly_sales
from item cross join store_sales cross join date_dim cross join store
where ss_item_sk = i_item_sk and
      ss_sold_date_sk = d_date_sk and
      ss_store_sk = s_store_sk and
      d_year in (1999) and
        ((i_category in ('Shoes','Men','Music') and
          i_class in ('kids','sports-apparel','classical')
         )
      or (i_category in ('Electronics','Women','Jewelry') and
          i_class in ('portable','fragrances','diamonds')
        ))
group by item.i_category, item.i_class, item.i_brand,
         store.s_store_name, store.s_company_name, date_dim.d_moy) tmp1
where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
order by sum_sales - avg_monthly_sales, s_store_name
limit 100;
