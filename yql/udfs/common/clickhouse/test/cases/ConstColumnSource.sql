SELECT cast(x as List<String>) FROM AS_TABLE(()->(ClickHouse::source(
    "SELECT timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600)) as x
     union all
     SELECT timeSlots(toDateTime('2013-01-01 12:20:00'), toUInt32(600)) as x"
)));
