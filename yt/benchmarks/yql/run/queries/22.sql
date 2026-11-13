-- TPC-DS 4.0.0 query 22; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  item.i_product_name
             ,item.i_brand
             ,item.i_class
             ,item.i_category
             ,avg(inv_quantity_on_hand) qoh
       from inventory
           cross join date_dim
           cross join item
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and d_month_seq between 1183 and 1183 + 11
       group by rollup(item.i_product_name
                       ,item.i_brand
                       ,item.i_class
                       ,item.i_category)
order by qoh, item.i_product_name, item.i_brand, item.i_class, item.i_category
limit 100;
