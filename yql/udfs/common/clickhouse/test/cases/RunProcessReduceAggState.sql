/* syntax version 1 */
use plato;

$callable1 = ($input)->( ClickHouse::run(
    $input, "select key,countState(1) as cnt from Input group by key") );
    
$callable2 = ($input)->( ClickHouse::run(
    $input, "select key,countMerge(cnt) as cnt from Input group by key") );

$x = process Input using $callable1(TableRows());

reduce $x on key using all $callable2(TableRows()); 

