#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/util/wait_for_tablets_state.h>

#include <library/unittest/registar.h>

#include <util/generic/algorithm.h>

using namespace NYT;
using namespace NYT::NTesting;

const auto WaitTabletsOptions =
    TWaitForTabletsStateOptions()
        .Timeout(TDuration::Seconds(30))
        .CheckInterval(TDuration::MilliSeconds(50));

void CreateTestTable(const IClientPtr& client, const TYPath& table, bool sorted = true)
{
    auto firstColumnSchema = TNode()("name", "key")("type", "int64");
    if (sorted) {
        firstColumnSchema["sort_order"] = "ascending";
    }

    client->Create(
        table,
        NT_TABLE,
        TCreateOptions()
        .Attributes(
            TNode()
            ("dynamic", true)
            ("schema", TNode()
             .Add(firstColumnSchema)
             .Add(TNode()("name", "value")("type", "string")))));
}

void CreateTestMulticolumnTable(const IClientPtr& client, const TYPath& table)
{
    client->Create(
        table,
        NT_TABLE,
        TCreateOptions()
        .Attributes(
            TNode()
            ("dynamic", true)
            ("schema", TNode()
             .Add(TNode()("name", "key")("type", "int64")("sort_order", "ascending"))
             .Add(TNode()("name", "value1")("type", "string"))
             .Add(TNode()("name", "value2")("type", "string")))));
}

void CreateTestAggregatingTable(const IClientPtr& client, const TYPath& table)
{
    client->Create(
        table,
        NT_TABLE,
        TCreateOptions()
        .Attributes(
            TNode()
            ("dynamic", true)
            ("schema", TNode()
             .Add(TNode()("name", "key")("type", "string")("sort_order", "ascending"))
             .Add(TNode()("name", "value")("type", "int64")("aggregate", "sum")))));
}

Y_UNIT_TEST_SUITE(TabletClient) {
    Y_UNIT_TEST(TestMountUnmount)
    {
        TTabletFixture fixture;
        auto client = fixture.Client();
        const TString tablePath = "//testing/test-mount-unmount";
        CreateTestTable(client, tablePath);

        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        client->RemountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);

        client->MountTable(tablePath, TMountTableOptions().Freeze(true));
        WaitForTabletsState(client, tablePath, TS_FROZEN, WaitTabletsOptions);

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestFreezeUnfreeze)
    {
        TTabletFixture fixture;
        auto client = fixture.Client();
        const TString tablePath = "//testing/test-freeze-unfreeze-1";
        CreateTestTable(client, tablePath);

        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        client->FreezeTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_FROZEN, WaitTabletsOptions);

        client->UnfreezeTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestReshard)
    {
        TTabletFixture fixture;
        auto client = fixture.Client();
        const TString tablePath = "//testing/test-reshard";
        CreateTestTable(client, tablePath);
        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        TNode::TListType rows;
        for (int i = 0; i < 16; ++i) {
            rows.push_back(TNode()("key", i)("value", ToString(i)));
        }
        client->InsertRows(tablePath, rows);

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);

        TVector<TKey> pivotKeys;
        pivotKeys.push_back(TKey());
        pivotKeys.push_back(4);
        pivotKeys.push_back(8);
        pivotKeys.push_back(12);

        client->ReshardTable(tablePath, pivotKeys);

        const auto& tabletList = client->Get(TStringBuilder() << tablePath << "/@tablets");
        UNIT_ASSERT_VALUES_EQUAL(tabletList.AsList().size(), 4);

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestInsertLookupDelete)
    {
        TTabletFixture fixture;
        auto client = fixture.Client();
        const TString tablePath = "//testing/test-insert-lookup-delete";
        CreateTestTable(client, tablePath);
        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        TNode::TListType rows = {
            TNode()("key", 1)("value", "one"),
            TNode()("key", 42)("value", "forty two"),
        };
        client->InsertRows(tablePath, rows);

        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 42), TNode()("key", 1)});
            UNIT_ASSERT_VALUES_EQUAL(result, TNode::TListType({rows[1], rows[0]}));
        }
        client->DeleteRows(tablePath, {});
        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 42), TNode()("key", 1)});
            UNIT_ASSERT_VALUES_EQUAL(result, TNode::TListType({rows[1], rows[0]}));
        }

        client->DeleteRows(tablePath, {TNode()("key", 42)});

        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 42), TNode()("key", 1)});
            UNIT_ASSERT_VALUES_EQUAL(result, TNode::TListType({rows[0]}));
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestTrimRows)
    {
        TTabletFixture fixture;
        auto client = fixture.Client();
        const TString tablePath = "//testing/test-trim-rows";
        CreateTestTable(client, tablePath, /* sorted = */ false);
        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        TNode::TListType rows = {
            TNode()("key", 1)("value", "one"),
            TNode()("key", 2)("value", "two"),
            TNode()("key", 3)("value", "three"),
            TNode()("key", 4)("value", "four"),
            TNode()("key", 5)("value", "five"),
        };
        client->InsertRows(tablePath, rows);

        client->TrimRows(tablePath, 0, 2);
        {
            auto result = client->SelectRows(
                "* from [//testing/test-trim-rows] where [$tablet_index] = 0 and [$row_index] between 0 and 2");
            UNIT_ASSERT_VALUES_EQUAL(result[0]["key"], rows[2]["key"]);
        }

        client->TrimRows(tablePath, 0, 3);
        {
            auto result = client->SelectRows(
                "* from [//testing/test-trim-rows] where [$tablet_index] = 0 and [$row_index] between 0 and 3");
            UNIT_ASSERT_VALUES_EQUAL(result[0]["key"], rows[3]["key"]);
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestAtomicityNoneInsert)
    {
        TTabletFixture fixture;
        auto client = fixture.Client();
        const TString tablePath = "//testing/test-atomicity-insert";
        CreateTestTable(client, tablePath);
        client->Set(tablePath + "/@atomicity", "none");
        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        TNode::TListType rows = {
            TNode()("key", 1)("value", "one"),
            TNode()("key", 42)("value", "forty two"),
        };
        UNIT_ASSERT_EXCEPTION(
            client->InsertRows(tablePath, rows),
            TErrorResponse);

        client->InsertRows(tablePath, rows, TInsertRowsOptions().Atomicity(EAtomicity::None));

        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 42), TNode()("key", 1)});
            UNIT_ASSERT_VALUES_EQUAL(result, TNode::TListType({rows[1], rows[0]}));
        }

        UNIT_ASSERT_EXCEPTION(
            client->DeleteRows(tablePath, {TNode()("key", 42)}),
            TErrorResponse);

        client->DeleteRows(tablePath, {TNode()("key", 42)},
            TDeleteRowsOptions().Atomicity(EAtomicity::None));

        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 42), TNode()("key", 1)});
            UNIT_ASSERT_VALUES_EQUAL(result, TNode::TListType({rows[0]}));
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestTimeoutType)
    {
        TTabletFixture fixture;
        auto client = fixture.Client();
        const TString tablePath = "//testing/test-timeout-type";
        CreateTestTable(client, tablePath);
        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        TNode::TListType rows = {
            TNode()("key", 1)("value", "one"),
            TNode()("key", 42)("value", "forty two"),
        };
        client->InsertRows(tablePath, rows);

        {
            auto result = client->LookupRows(tablePath,
                {TNode()("key", 42), TNode()("key", 1)},
                NYT::TLookupRowsOptions().Timeout(TDuration::Seconds(1)));
            UNIT_ASSERT_VALUES_EQUAL(result, TNode::TListType({rows[1], rows[0]}));
        }

        {
            auto result = client->SelectRows("* from [//testing/test-timeout-type]", NYT::TSelectRowsOptions().Timeout(TDuration::Seconds(1)));
            //Sort(result.begin(), result.end(), [] (const TNode& lhs, const TNode& rhs) {
                    //return lhs["key"].AsInt64() < rhs["key"].AsInt64();
                //});
            UNIT_ASSERT_VALUES_EQUAL(result, rows);
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestUpdateInsert)
    {
        TTabletFixture fixture;
        auto client = fixture.Client();
        const TString tablePath = "//testing/test-update-insert";
        CreateTestMulticolumnTable(client, tablePath);
        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        client->InsertRows(tablePath, {TNode()("key", 1)("value1", "one")("value2", "odin")});

        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 1)});
            UNIT_ASSERT_VALUES_EQUAL(result, std::vector<TNode>{TNode()("key", 1)("value1", "one")("value2", "odin")});
        }

        client->InsertRows(tablePath, {TNode()("key", 1)("value1", "two")}, TInsertRowsOptions().Update(true));
        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 1)});
            UNIT_ASSERT_VALUES_EQUAL(result, std::vector<TNode>{TNode()("key", 1)("value1", "two")("value2", "odin")});
        }

        client->InsertRows(tablePath, {TNode()("key", 1)("value2", "dva")});
        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 1)});
            UNIT_ASSERT_VALUES_EQUAL(result, std::vector<TNode>{TNode()("key", 1)("value1", TNode::CreateEntity())("value2", "dva")});
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestAggregateInsert)
    {
        TTabletFixture fixture;
        auto client = fixture.Client();
        const TString tablePath = "//testing/test-aggregate-insert";
        CreateTestAggregatingTable(client, tablePath);
        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        client->InsertRows(tablePath, {TNode()("key", "one")("value", 5)});

        {
            auto result = client->LookupRows(tablePath, {TNode()("key", "one")});
            UNIT_ASSERT_VALUES_EQUAL(result, std::vector<TNode>{TNode()("key", "one")("value", 5)});
        }

        client->InsertRows(tablePath, {TNode()("key", "one")("value", 5)}, TInsertRowsOptions().Aggregate(true));
        {
            auto result = client->LookupRows(tablePath, {TNode()("key", "one")});
            UNIT_ASSERT_VALUES_EQUAL(result, std::vector<TNode>{TNode()("key", "one")("value", 10)});
        }

        client->InsertRows(tablePath, {TNode()("key", "one")("value", 5)});
        {
            auto result = client->LookupRows(tablePath, {TNode()("key", "one")});
            UNIT_ASSERT_VALUES_EQUAL(result, std::vector<TNode>{TNode()("key", "one")("value", 5)});
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }
}
