#include <library/cpp/testing/hook/hook.h>

#include <yt/server/clickhouse_server/clickhouse_singletons.h>

Y_TEST_HOOK_BEFORE_RUN(GTEST_CHYT_SETUP)
{
    NYT::NClickHouseServer::RegisterClickHouseSingletons();
}
