/* syntax version 1 */
select
    cast(ClickHouse::array(Uuid('00010203-0405-0607-0809-0a0b0c0d0e0f')) as List<String>),
    cast(ClickHouse::toUUID('00010203-0405-0607-0809-0a0b0c0d0e0f') as String);