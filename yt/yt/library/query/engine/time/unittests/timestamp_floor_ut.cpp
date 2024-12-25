#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/query/engine/time/dates.h>

#include <ctime>


namespace NYT::NQueryClient::NRoutines {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 Step = 12345;
constexpr i64 TestedWrapRange = 10'000'000;

int DaysFromMonday(int weekDayInWorstCountries)
{
    return (weekDayInWorstCountries + 6) % 7;
}

int DaysInYear(int year)
{
    return 365 + (year % 4 == 0);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TimestampFloorUtc, Week)
{
    auto check = [] (i64 timestamp) {
        i64 result = TimestampFloorWeek(timestamp);
        i64 prev = result - 1;

        tm timeinfo;
        tm resultTimeinfo;
        tm prevTimeinfo;

        gmtime_r(&timestamp, &timeinfo);
        gmtime_r(&result, &resultTimeinfo);
        gmtime_r(&prev, &prevTimeinfo);

        EXPECT_EQ(0, DaysFromMonday(resultTimeinfo.tm_wday));
        EXPECT_EQ(6, DaysFromMonday(prevTimeinfo.tm_wday));
        EXPECT_LE((timeinfo.tm_yday - resultTimeinfo.tm_yday + DaysInYear(resultTimeinfo.tm_year)) % DaysInYear(resultTimeinfo.tm_year), 7);
    };

    for (i64 timestamp = MinLutUtcTimestamp - TestedWrapRange; timestamp < MaxLutUtcTimestamp + TestedWrapRange; timestamp += Step) {
        check(timestamp);
    }

    check(MinLutUtcTimestamp);
    check(MinLutUtcTimestamp - 1);
    check(MinLutUtcTimestamp + 1);

    check(MaxLutUtcTimestamp);
    check(MaxLutUtcTimestamp - 1);
    check(MaxLutUtcTimestamp + 1);
}

TEST(TimestampFloorUtc, Month)
{
    auto check = [] (i64 timestamp) {
        i64 result = TimestampFloorMonth(timestamp);
        i64 prev = result - 1;

        tm timeinfo;
        tm resultTimeinfo;
        tm prevtimeinfo;

        gmtime_r(&timestamp, &timeinfo);
        gmtime_r(&result, &resultTimeinfo);
        gmtime_r(&prev, &prevtimeinfo);

        EXPECT_EQ(timeinfo.tm_yday - timeinfo.tm_mday, resultTimeinfo.tm_yday - resultTimeinfo.tm_mday);
        EXPECT_NE(prevtimeinfo.tm_yday - prevtimeinfo.tm_mday, resultTimeinfo.tm_yday - resultTimeinfo.tm_mday);
    };

    for (i64 timestamp = MinLutUtcTimestamp - TestedWrapRange; timestamp < MaxLutUtcTimestamp + TestedWrapRange; timestamp += Step) {
        check(timestamp);
    }

    check(MinLutUtcTimestamp);
    check(MinLutUtcTimestamp - 1);
    check(MinLutUtcTimestamp + 1);

    check(MaxLutUtcTimestamp);
    check(MaxLutUtcTimestamp - 1);
    check(MaxLutUtcTimestamp + 1);
}

TEST(TimestampFloorUtc, Quarter)
{
    auto check = [] (i64 timestamp) {
        i64 result = TimestampFloorQuarter(timestamp);
        i64 prev = result - 1;

        tm timeinfo;
        tm resultTimeinfo;
        tm prevtimeinfo;

        gmtime_r(&timestamp, &timeinfo);
        gmtime_r(&result, &resultTimeinfo);
        gmtime_r(&prev, &prevtimeinfo);

        EXPECT_EQ(timeinfo.tm_mon - timeinfo.tm_mon % 4, resultTimeinfo.tm_mon - resultTimeinfo.tm_mon % 4);
        EXPECT_NE(prevtimeinfo.tm_mon - prevtimeinfo.tm_mon % 4, resultTimeinfo.tm_mon - resultTimeinfo.tm_mon % 4);
    };

    for (i64 timestamp = MinLutUtcTimestamp - TestedWrapRange; timestamp < MaxLutUtcTimestamp + TestedWrapRange; timestamp += Step) {
        check(timestamp);
    }

    check(MinLutUtcTimestamp);
    check(MinLutUtcTimestamp - 1);
    check(MinLutUtcTimestamp + 1);

    check(MaxLutUtcTimestamp);
    check(MaxLutUtcTimestamp - 1);
    check(MaxLutUtcTimestamp + 1);
}

TEST(TimestampFloorUtc, Year)
{
    auto check = [] (i64 timestamp) {
        i64 result = TimestampFloorYear(timestamp);
        i64 prev = result - 1;

        tm timeinfo;
        tm resultTimeinfo;
        tm prevtimeinfo;

        gmtime_r(&timestamp, &timeinfo);
        gmtime_r(&result, &resultTimeinfo);
        gmtime_r(&prev, &prevtimeinfo);

        EXPECT_EQ(timeinfo.tm_year, resultTimeinfo.tm_year);
        EXPECT_NE(prevtimeinfo.tm_year, resultTimeinfo.tm_year);
    };

    for (i64 timestamp = MinLutUtcTimestamp - TestedWrapRange; timestamp < MaxLutUtcTimestamp + TestedWrapRange; timestamp += Step) {
        check(timestamp);
    }

    check(MinLutUtcTimestamp);
    check(MinLutUtcTimestamp - 1);
    check(MinLutUtcTimestamp + 1);

    check(MaxLutUtcTimestamp);
    check(MaxLutUtcTimestamp - 1);
    check(MaxLutUtcTimestamp + 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TimestampFloorLocaltime, Day)
{
    auto check = [] (i64 timestamp) {
        i64 result = TimestampFloorDayLocaltime(timestamp);
        i64 prev = result - 1;

        tm timeinfo;
        tm resultTimeinfo;
        tm prevTimeinfo;

        localtime_r(&timestamp, &timeinfo);
        localtime_r(&result, &resultTimeinfo);
        localtime_r(&prev, &prevTimeinfo);

        EXPECT_EQ(timeinfo.tm_yday, resultTimeinfo.tm_yday);
        EXPECT_NE(prevTimeinfo.tm_yday, resultTimeinfo.tm_yday);
    };

    for (i64 timestamp = MinLutLocaltimeTimestamp - TestedWrapRange; timestamp < MaxLutLocaltimeTimestamp + TestedWrapRange; timestamp += Step) {
        check(timestamp);
    }

    check(MinLutLocaltimeTimestamp);
    check(MinLutLocaltimeTimestamp - 1);
    check(MinLutLocaltimeTimestamp + 1);

    check(MaxLutLocaltimeTimestamp);
    check(MaxLutLocaltimeTimestamp - 1);
    check(MaxLutLocaltimeTimestamp + 1);
}

TEST(TimestampFloorLocaltime, Week)
{
    auto check = [] (i64 timestamp) {
        i64 result = TimestampFloorWeekLocaltime(timestamp);
        i64 prev = result - 1;

        tm timeinfo;
        tm resultTimeinfo;
        tm prevTimeinfo;

        localtime_r(&timestamp, &timeinfo);
        localtime_r(&result, &resultTimeinfo);
        localtime_r(&prev, &prevTimeinfo);

        EXPECT_EQ(0, DaysFromMonday(resultTimeinfo.tm_wday));
        EXPECT_EQ(6, DaysFromMonday(prevTimeinfo.tm_wday));
        EXPECT_LE((timeinfo.tm_yday - resultTimeinfo.tm_yday + DaysInYear(resultTimeinfo.tm_year)) % DaysInYear(resultTimeinfo.tm_year), 7);
    };

    for (i64 timestamp = MinLutLocaltimeTimestamp - TestedWrapRange; timestamp < MaxLutLocaltimeTimestamp + TestedWrapRange; timestamp += Step) {
        check(timestamp);
    }

    check(MinLutLocaltimeTimestamp);
    check(MinLutLocaltimeTimestamp - 1);
    check(MinLutLocaltimeTimestamp + 1);

    check(MaxLutLocaltimeTimestamp);
    check(MaxLutLocaltimeTimestamp - 1);
    check(MaxLutLocaltimeTimestamp + 1);
}

TEST(TimestampFloorLocaltime, Month)
{
    auto check = [] (i64 timestamp) {
        i64 result = TimestampFloorMonthLocaltime(timestamp);
        i64 prev = result - 1;

        tm timeinfo;
        tm resultTimeinfo;
        tm prevtimeinfo;

        localtime_r(&timestamp, &timeinfo);
        localtime_r(&result, &resultTimeinfo);
        localtime_r(&prev, &prevtimeinfo);

        EXPECT_EQ(timeinfo.tm_yday - timeinfo.tm_mday, resultTimeinfo.tm_yday - resultTimeinfo.tm_mday);
        EXPECT_NE(prevtimeinfo.tm_yday - prevtimeinfo.tm_mday, resultTimeinfo.tm_yday - resultTimeinfo.tm_mday);
    };

    for (i64 timestamp = MinLutLocaltimeTimestamp - TestedWrapRange; timestamp < MaxLutLocaltimeTimestamp + TestedWrapRange; timestamp += Step) {
        check(timestamp);
    }

    check(MinLutLocaltimeTimestamp);
    check(MinLutLocaltimeTimestamp - 1);
    check(MinLutLocaltimeTimestamp + 1);

    check(MaxLutLocaltimeTimestamp);
    check(MaxLutLocaltimeTimestamp - 1);
    check(MaxLutLocaltimeTimestamp + 1);
}

TEST(TimestampFloorLocaltime, Quarter)
{
    auto check = [] (i64 timestamp) {
        i64 result = TimestampFloorQuarterLocaltime(timestamp);
        i64 prev = result - 1;

        tm timeinfo;
        tm resultTimeinfo;
        tm prevtimeinfo;

        localtime_r(&timestamp, &timeinfo);
        localtime_r(&result, &resultTimeinfo);
        localtime_r(&prev, &prevtimeinfo);

        EXPECT_EQ(timeinfo.tm_mon - timeinfo.tm_mon % 4, resultTimeinfo.tm_mon - resultTimeinfo.tm_mon % 4);
        EXPECT_NE(prevtimeinfo.tm_mon - prevtimeinfo.tm_mon % 4, resultTimeinfo.tm_mon - resultTimeinfo.tm_mon % 4);
    };

    for (i64 timestamp = MinLutLocaltimeTimestamp - TestedWrapRange; timestamp < MaxLutLocaltimeTimestamp + TestedWrapRange; timestamp += Step) {
        check(timestamp);
    }

    check(MinLutLocaltimeTimestamp);
    check(MinLutLocaltimeTimestamp - 1);
    check(MinLutLocaltimeTimestamp + 1);

    check(MaxLutLocaltimeTimestamp);
    check(MaxLutLocaltimeTimestamp - 1);
    check(MaxLutLocaltimeTimestamp + 1);
}

TEST(TimestampFloorLocaltime, Year)
{
    auto check = [] (i64 timestamp) {
        i64 result = TimestampFloorYearLocaltime(timestamp);
        i64 prev = result - 1;

        tm timeinfo;
        tm resultTimeinfo;
        tm prevtimeinfo;

        localtime_r(&timestamp, &timeinfo);
        localtime_r(&result, &resultTimeinfo);
        localtime_r(&prev, &prevtimeinfo);

        EXPECT_EQ(timeinfo.tm_year, resultTimeinfo.tm_year);
        EXPECT_NE(prevtimeinfo.tm_year, resultTimeinfo.tm_year);
    };

    for (i64 timestamp = MinLutLocaltimeTimestamp - TestedWrapRange; timestamp < MaxLutLocaltimeTimestamp + TestedWrapRange; timestamp += Step) {
        check(timestamp);
    }

    check(MinLutLocaltimeTimestamp);
    check(MinLutLocaltimeTimestamp - 1);
    check(MinLutLocaltimeTimestamp + 1);

    check(MaxLutLocaltimeTimestamp);
    check(MaxLutLocaltimeTimestamp - 1);
    check(MaxLutLocaltimeTimestamp + 1);
}

////////////////////////////////////////////////////////////////////////////////
} // namespace NYT::NQueryClient::NRoutines
