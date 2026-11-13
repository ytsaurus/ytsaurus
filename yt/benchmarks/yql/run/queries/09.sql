-- TPC-DS 4.0.0 query 9; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
$count_1_20 = (select count(*)
    from store_sales
    where ss_quantity between 1 and 20);

$avg_then_1_20 = (select avg(ss_ext_tax)
    from store_sales
    where ss_quantity between 1 and 20);

$avg_else_1_20 = (select avg(ss_net_paid)
    from store_sales
    where ss_quantity between 1 and 20);


$count_21_40 = (select count(*)
    from store_sales
    where ss_quantity between 21 and 40);

$avg_then_21_40 = (select avg(ss_ext_tax)
    from store_sales
    where ss_quantity between 21 and 40);

$avg_else_21_40 = (select avg(ss_net_paid)
    from store_sales
    where ss_quantity between 21 and 40);


$count_41_60 = (select count(*)
    from store_sales
    where ss_quantity between 41 and 60);

$avg_then_41_60 = (select avg(ss_ext_tax)
    from store_sales
    where ss_quantity between 41 and 60);

$avg_else_41_60 = (select avg(ss_net_paid)
    from store_sales
    where ss_quantity between 41 and 60);


$count_61_80 = (select count(*)
    from store_sales
    where ss_quantity between 61 and 80);

$avg_then_61_80 = (select avg(ss_ext_tax)
    from store_sales
    where ss_quantity between 61 and 80);

$avg_else_61_80 = (select avg(ss_net_paid)
    from store_sales
    where ss_quantity between 61 and 80);


$count_81_100 = (select count(*)
    from store_sales
    where ss_quantity between 81 and 100);

$avg_then_81_100 = (select avg(ss_ext_tax)
    from store_sales
    where ss_quantity between 81 and 100);

$avg_else_81_100 = (select avg(ss_net_paid)
    from store_sales
    where ss_quantity between 81 and 100);


select case when $count_1_20 > 12171089
            then $avg_then_1_20
            else $avg_else_1_20 end bucket1 ,
       case when $count_21_40 > 111772806
            then $avg_then_21_40
            else $avg_else_21_40 end bucket2,
       case when $count_41_60 > 23211533
            then $avg_then_41_60
            else $avg_else_41_60 end bucket3,
       case when $count_61_80 > 56475617
            then $avg_then_61_80
            else $avg_else_61_80 end bucket4,
       case when $count_81_100 > 43543186
            then $avg_then_81_100
            else $avg_else_81_100 end bucket5
from reason
where r_reason_sk = 1
;
