USE plato;
pragma UseBlocks;

select
  a,
  ClickHouse::reverse(a),
  ClickHouse::upper(b),
  --supported in 22.3+
  --ClickHouse::tupleNegate(c),
  c,
  ClickHouse::tupleElement(d, 2u),
  ClickHouse::tupleElement(e, 1u),
from Input
order by a;
