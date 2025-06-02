/* syntax version 1 */
use plato;
$callable = ($input)->( ClickHouse::run(
    $input, "select key,count(*) as cnt from Input group by key") );

select * from (
    reduce Input on key using all $callable(TableRows())
) order by key;
