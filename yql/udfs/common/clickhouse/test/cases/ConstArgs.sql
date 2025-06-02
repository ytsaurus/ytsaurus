/* syntax version 1 */
select 
    ClickHouse::format('{} {}', 'Hello', 'World'),
    ClickHouse::splitByChar(" ","a b c"),
    ClickHouse::multiSearchFirstPosition('fedcba',['q','b','z']),
    ClickHouse::multiSearchFirstIndex('fedcba',['q','b','z']),
    ClickHouse::multiSearchFirstPosition('fedcba',['q','x','z']),
    ClickHouse::multiSearchFirstIndex('fedcba',['q','x','z']);

