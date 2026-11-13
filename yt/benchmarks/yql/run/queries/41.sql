-- TPC-DS 4.0.0 query 41; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
$item_cnt_by_manufact = (select i_manufact, count(*) as item_cnt
        from item
        where (
        ((i_category = 'Women' and
        (i_color = 'light' or i_color = 'yellow') and
        (i_units = 'Pallet' or i_units = 'Box') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'firebrick' or i_color = 'cream') and
        (i_units = 'Oz' or i_units = 'Carton') and
        (i_size = 'petite' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'magenta' or i_color = 'sandy') and
        (i_units = 'Ounce' or i_units = 'Cup') and
        (i_size = 'economy' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'frosted' or i_color = 'plum') and
        (i_units = 'Tsp' or i_units = 'Bundle') and
        (i_size = 'large' or i_size = 'medium')
        ))) or
       (
        ((i_category = 'Women' and
        (i_color = 'orchid' or i_color = 'chocolate') and
        (i_units = 'Pound' or i_units = 'Lb') and
        (i_size = 'large' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'slate' or i_color = 'linen') and
        (i_units = 'Gross' or i_units = 'Unknown') and
        (i_size = 'petite' or i_size = 'extra large')
        ) or
        (i_category = 'Men' and
        (i_color = 'gainsboro' or i_color = 'dim') and
        (i_units = 'Ton' or i_units = 'Dozen') and
        (i_size = 'economy' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'ivory' or i_color = 'navy') and
        (i_units = 'Tbl' or i_units = 'Bunch') and
        (i_size = 'large' or i_size = 'medium')
        )))
        group by i_manufact);

select  distinct(i_product_name)
 from item i1 join $item_cnt_by_manufact i2 using (i_manufact)
 where i_manufact_id between 744 and 744+40
   and i2.item_cnt > 0
 order by i_product_name
 limit 100;
