/* syntax version 1 */
$s = <|x:'[1,2]'j,y:'[1;2;3]'y,z:'hello'u|>;
select * from as_table(()->(
    ClickHouse::run([$s], 
    "select JSONLength(x),YSONLength(y),lengthUTF8(z) from Input")));

select ClickHouse::JSONLength($s.x), ClickHouse::YSONLength($s.y), ClickHouse::lengthUTF8($s.z);
