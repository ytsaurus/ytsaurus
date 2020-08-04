#include <library/cpp/testing/gtest/gtest.h>

#include <yt/server/clickhouse_server/clickhouse_singletons.h>

Y_GTEST_HOOK_BEFORE_RUN(GTEST_CHYT_SETUP)
{
    NYT::NClickHouseServer::RegisterClickHouseSingletons();
}
