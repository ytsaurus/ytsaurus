//%NUM_MASTERS=1
//%NUM_NODES=3
//%NUM_SCHEDULERS=0
//%DRIVER_BACKENDS=['rpc']
//%ENABLE_RPC_PROXY=True
//%DELTA_MASTER_CONFIG={"object_service":{"timeout_backoff_lead_time":100}}

#include "yt/yt/tests/cpp/api_test_base.h"
#include "yt/yt/tests/cpp/modify_rows_test.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ytree/convert.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NYTree;

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

class TMultiLookupTest
    : public TDynamicTablesTestBase
{
public:
    static void SetUpTestCase()
    {
        auto configPath = TString(std::getenv("YT_DRIVER_CONFIG_PATH"));
        YT_VERIFY(configPath);
        IMapNodePtr config;
        {
            TIFStream configInStream(configPath);
            config = ConvertToNode(&configInStream)->AsMap();
        }
        config->AddChild("enable_multi_lookup", ConvertToNode(true));
        {
            TOFStream configOutStream(configPath);
            configOutStream << ConvertToYsonString(config).ToString() << Endl;
        }

        TDynamicTablesTestBase::SetUpTestCase();

        CreateTable(
            "//tmp/multi_lookup_test", // tablePath
            "[" // schema
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=v1;type=int64};]"
        );
    }

    static void TearDownTestCase()
    {
        TDynamicTablesTestBase::TearDownTestCase();
    }
};

TEST_F(TMultiLookupTest, TestMultiLookup)
{
    WriteUnversionedRow(
        {"k0", "v1"},
        "<id=0> 0; <id=1> 0;");
    WriteUnversionedRow(
        {"k0", "v1"},
        "<id=0> 1; <id=1> 1");

    auto key0 = PrepareUnversionedRow(
        {"k0", "v1"},
        "<id=0> 0;");
    auto key1 = PrepareUnversionedRow(
        {"k0", "v1"},
        "<id=0; ts=2> 1;");

    std::vector<TMultiLookupSubrequest> subrequests;
    subrequests.push_back({
        Table_,
        std::get<1>(key0),
        std::get<0>(key0),
        TLookupRowsOptions()});
    subrequests.push_back({
        Table_,
        std::get<1>(key1),
        std::get<0>(key1),
        TLookupRowsOptions()});

    auto rowsets = WaitFor(Client_->MultiLookup(
        subrequests,
        TMultiLookupOptions()))
        .ValueOrThrow();

    ASSERT_EQ(2, rowsets.size());

    ASSERT_EQ(1, rowsets[0]->GetRows().Size());
    ASSERT_EQ(1, rowsets[1]->GetRows().Size());

    auto expected = ToString(YsonToSchemalessRow("<id=0> 0; <id=1> 0;"));
    auto actual = ToString(rowsets[0]->GetRows()[0]);
    EXPECT_EQ(expected, actual);

    expected = ToString(YsonToSchemalessRow("<id=0> 1; <id=1> 1;"));
    actual = ToString(rowsets[1]->GetRows()[0]);
    EXPECT_EQ(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT

