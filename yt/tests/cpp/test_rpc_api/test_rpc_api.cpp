//%NUM_MASTERS=1
//%NUM_NODES=3
//%NUM_SCHEDULERS=0
//%DRIVER_BACKENDS=['rpc']
//%ENABLE_RPC_PROXY=True
//%DELTA_MASTER_CONFIG={"object_service":{"timeout_backoff_lead_time":100}}

#include "yt/tests/cpp/api_test_base.h"
#include "yt/tests/cpp/modify_rows_test.h"

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NCppTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST_F(TModifyRowsTest, TestReordering)
{
    const int rowCount = 20;

    for (int i = 0; i < rowCount; ++i) {
        WriteSimpleRow(i, i + 10);
        WriteSimpleRow(i, i + 11);
    }
    SyncCommit();

    std::vector<std::pair<i64, i64>> expected;
    for (int i = 0; i < rowCount; ++i) {
        expected.emplace_back(i, i + 11);
    }
    CheckTableContents(expected);
}

TEST_F(TModifyRowsTest, TestIgnoringSeqNumbers)
{
    WriteSimpleRow(0, 10, 4);
    WriteSimpleRow(1, 11, 3);
    WriteSimpleRow(0, 12, 2);
    WriteSimpleRow(1, 13, -1);
    WriteSimpleRow(0, 14);
    WriteSimpleRow(1, 15, 100500);
    SyncCommit();

    CheckTableContents({{0, 14}, {1, 15}});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT

