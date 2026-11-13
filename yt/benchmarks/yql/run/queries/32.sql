-- TPC-DS 4.0.0 query 32; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$avg_discount_by_item = (
         select
            item.i_item_sk i_item_sk,
            item.i_manufact_id i_manufact_id,
            avg(cs_ext_discount_amt) as avg_discout
         from
           catalog_sales
           cross join date_dim
           cross join item
         where cast (d_date as date) between cast('1999-02-22' as date) and
                             (cast('1999-02-22' as date) + DateTime::IntervalFromDays(90))
          and d_date_sk = cs_sold_date_sk
          and cs_item_sk = i_item_sk
          group by item.i_item_sk, item.i_manufact_id
      );

select  sum(cs_ext_discount_amt)  as `excess discount amount`
from
   catalog_sales cs
   cross join item
   cross join date_dim
   join $avg_discount_by_item adi on cs.cs_item_sk = adi.i_item_sk and item.i_manufact_id = adi.i_manufact_id
where
item.i_manufact_id = 283
and item.i_item_sk = cs.cs_item_sk
and cast (d_date as date) between cast('1999-02-22' as date) and
        (cast('1999-02-22' as date) + DateTime::IntervalFromDays(90))
and d_date_sk = cs_sold_date_sk
and cs_ext_discount_amt
     > 1.3 * adi.avg_discout
limit 100;
