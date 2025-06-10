/* syntax version 1 */
select * from as_table(()->(ClickHouse::source("
    select cast('foo' as Enum8('foo'=-1,'bar'=2)) as x,
           cast('bar' as Enum16('foo'=-1,'bar'=2000)) as y
")));

$t = Enum<'a','b','c'>;
select ClickHouse::array(Enum('a',$t),Enum('b',$t),Enum('c',$t));
