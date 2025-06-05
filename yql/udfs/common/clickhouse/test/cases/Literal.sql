/* syntax version 1 */
select 
    ClickHouse::tupleElement((1,"foo"),1ut),
    ClickHouse::tupleElement((1,"foo"),2ut),
    ClickHouse::toFixedString("ab",3u);
