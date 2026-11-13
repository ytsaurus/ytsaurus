-- TPC-DS 4.0.0 query 92; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$bla = (
         SELECT
         web_sales.ws_item_sk bla_item_sk,
            avg(ws_ext_discount_amt)  bla_ext_discount_amt
         FROM
            web_sales
           cross join date_dim
         WHERE
        cast(d_date as date) between cast('1998-02-02' as date) and
                             (cast('1998-02-02' as date) + DateTime::IntervalFromDays(90))
          and d_date_sk = ws_sold_date_sk
          group by web_sales.ws_item_sk
      );

select
   sum(ws_ext_discount_amt)  as `Excess Discount Amount`
from
    web_sales
   cross join item
   cross join date_dim
   join $bla bla on (item.i_item_sk = bla.bla_item_sk)
where
i_manufact_id = 992
and i_item_sk = ws_item_sk
and cast(d_date as date) between cast('1998-02-02' as date) and
        (cast('1998-02-02' as date) + DateTime::IntervalFromDays(90))
and d_date_sk = ws_sold_date_sk
and ws_ext_discount_amt
     > 1.3 * bla.bla_ext_discount_amt
order by `Excess Discount Amount`
limit 100;
