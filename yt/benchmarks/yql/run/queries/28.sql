-- TPC-DS 4.0.0 query 28; dsqgen -SCALE 3000 -RNGSEED 19620718 -STREAMS 1.
-- NB: Subqueries
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 85 and 85+10
             or ss_coupon_amt between 725 and 725+1000
             or ss_wholesale_cost between 18 and 18+20)) B1 cross join
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 6 and 6+10
          or ss_coupon_amt between 11704 and 11704+1000
          or ss_wholesale_cost between 15 and 15+20)) B2 cross join
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 72 and 72+10
          or ss_coupon_amt between 5172 and 5172+1000
          or ss_wholesale_cost between 27 and 27+20)) B3 cross join
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 104 and 104+10
          or ss_coupon_amt between 2440 and 2440+1000
          or ss_wholesale_cost between 65 and 65+20)) B4 cross join
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 153 and 153+10
          or ss_coupon_amt between 1350 and 1350+1000
          or ss_wholesale_cost between 3 and 3+20)) B5 cross join
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 64 and 64+10
          or ss_coupon_amt between 2991 and 2991+1000
          or ss_wholesale_cost between 7 and 7+20)) B6
limit 100;
