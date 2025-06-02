/* syntax version 1 */
select 
    ClickHouse::arrayFlatten([[[1]],[[2],[3]]]),
    ClickHouse::array([4],[5,6]),
    ClickHouse::array([4,5],[6]),
    ClickHouse::arrayReverse([[4,5],[6]]),
    ClickHouse::arrayReverse([[4],[5,6]]),
    ClickHouse::length([[4],[5,6]]),
    ClickHouse::arraySlice([[4],[5,6],[7,8,9],[10,11,12,13]],2,2);