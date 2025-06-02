/* syntax version 1 */
$callable1 = ($input)->( ClickHouse::run(
    $input, "select region,countState(1) as cnt from Input group by region") );
    
$callable2 = ($input)->( ClickHouse::run(
    $input, "select region,countMerge(cnt) as cnt from Input group by region") );

$x = process (select * from as_table([<|region:1, age:10|>, <|region:1, age:11|>, <|region:2, age:20|>])) using $callable1(TableRows());

reduce $x on region using all $callable2(TableRows()); 

