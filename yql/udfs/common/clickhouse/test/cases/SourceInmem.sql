/* syntax version 1 */
select * from as_table(()->(ClickHouse::source("select 1")));
select * from as_table(()->(ClickHouse::source("select * from system.numbers limit 5")));
