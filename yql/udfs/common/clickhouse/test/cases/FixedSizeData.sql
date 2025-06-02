/* syntax version 1 */
use plato;
select
   ClickHouse::abs(-key),
   ClickHouse::lcm(key,value+1)
from (
   select
      Unwrap(cast(key as Int32)) as key,
      Unwrap(cast(subkey as Int32)) as subkey,
      Unwrap(cast(value as Int32)) as value
   from Input
);
