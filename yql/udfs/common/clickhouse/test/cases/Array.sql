/* syntax version 1 */
select
  ClickHouse::length([1,2,3]),
  ClickHouse::length(["a","bc","def","ghij"]),
  ClickHouse::length([]),
  ClickHouse::length(ListCreate(Int32)),
  ClickHouse::length([null]),
  ClickHouse::length([ClickHouse::toNullable(1), ClickHouse::toNullable(2), ClickHouse::toNullable(3)]),
  ClickHouse::length([ClickHouse::toNullable("foo"), ClickHouse::toNullable("bar")]),
  
  ClickHouse::reverse([1,2,3]),
  ClickHouse::reverse(["a","bc","def","ghij"]),
  ClickHouse::reverse([]),
  ClickHouse::reverse(ListCreate(Int32)),
  ClickHouse::reverse([null]),
  ClickHouse::reverse([ClickHouse::toNullable(1), ClickHouse::toNullable(2), ClickHouse::toNullable(3)]),
  ClickHouse::reverse([ClickHouse::toNullable("foo"), ClickHouse::toNullable("bar")]);
