/* syntax version 1 */
select 
    ClickHouse::regionToCity(98546u),
    ClickHouse::regionToCity(98546u,'ua'),
    ClickHouse::regionToName(2u),
    ClickHouse::regionToName(2u,'en');
