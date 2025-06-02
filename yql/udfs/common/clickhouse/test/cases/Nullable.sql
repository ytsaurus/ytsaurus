/* syntax version 1 */
select
    ClickHouse::ifNull(1,2), 
    ClickHouse::ifNull(null,3),
    ClickHouse::ifNull(just(5),6),
    ClickHouse::ifNull(nothing(Int32?),7),
    ClickHouse::abs(just(-1)),
    ClickHouse::abs(nothing(Int32?)),
    ClickHouse::abs(null),
