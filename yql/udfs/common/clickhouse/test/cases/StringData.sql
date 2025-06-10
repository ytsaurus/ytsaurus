/* syntax version 1 */
use plato;
select
   ClickHouse::upper(key),
   ClickHouse::lower(key),
   ClickHouse::cityHash64('Hello, world!')
from Input;
