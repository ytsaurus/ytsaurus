/* syntax version 1 */
select
   ClickHouse::toDateTime(cast(Datetime("2016-10-01T15:00:00Z") as Uint32)),
   ClickHouse::toDate(cast(Date("2016-10-01") as Uint16)),
   cast(ClickHouse::addHours(
        ClickHouse::toDateTime(cast(Datetime("2016-10-01T15:00:00Z") as Uint32)),-1) as string),
   cast(ClickHouse::addDays(
        ClickHouse::toDate(cast(Date("2016-10-01") as Uint16)),2) as string);
