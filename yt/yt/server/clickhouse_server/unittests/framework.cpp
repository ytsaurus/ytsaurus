#include <library/cpp/testing/hook/hook.h>

#include <yt/server/clickhouse_server/clickhouse_singletons.h>
#include <yt/server/clickhouse_server/data_type_boolean.h>

Y_TEST_HOOK_BEFORE_RUN(GTEST_CHYT_SETUP)
{
    NYT::NClickHouseServer::RegisterClickHouseSingletons();
    NYT::NClickHouseServer::RegisterDataTypeBoolean();
}
