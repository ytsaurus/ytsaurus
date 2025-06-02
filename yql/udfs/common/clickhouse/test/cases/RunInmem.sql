/* syntax version 1 */
select * from as_table(()->(
    ClickHouse::run([<|y:1,x:2|>,<|y:3,x:4|>], 
    "select x+1 as b,x*y as a,cast(10*x+y as String) || '_foo' as c from Input")));

