/* syntax version 1 */
select 
    ClickHouse::tuple(null,(-1,2u),just(3.0),["foo","bar"]),
    ClickHouse::array((null,(-1,2u),just(3.0),["foo","bar"]));
