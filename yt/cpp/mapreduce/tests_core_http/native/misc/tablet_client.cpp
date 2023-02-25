#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/util/wait_for_tablets_state.h>

#include <library/cpp/testing/unittest/registar.h>

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
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-mount-unmount";
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
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-freeze-unfreeze-1";
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
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-reshard";
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

        const auto& tabletList = client->Get(::TStringBuilder() << tablePath << "/@tablets");
        UNIT_ASSERT_VALUES_EQUAL(tabletList.AsList().size(), 4);

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestInsertLookupDelete)
    {
        TTabletFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-insert-lookup-delete";
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
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-trim-rows";
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
                "* from [" + tablePath + "] where [$tablet_index] = 0 and [$row_index] between 0 and 2");
            UNIT_ASSERT_VALUES_EQUAL(result[0]["key"], rows[2]["key"]);
        }

        client->TrimRows(tablePath, 0, 3);
        {
            auto result = client->SelectRows(
                "* from [" + tablePath + "] where [$tablet_index] = 0 and [$row_index] between 0 and 3");
            UNIT_ASSERT_VALUES_EQUAL(result[0]["key"], rows[3]["key"]);
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestAtomicityNoneInsert)
    {
        TTabletFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-atomicity-insert";
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
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-timeout-type";
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
            auto result = client->SelectRows(
                "* from [" + tablePath + "]",
                NYT::TSelectRowsOptions().Timeout(TDuration::Seconds(1)));
            UNIT_ASSERT_VALUES_EQUAL(result, rows);
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestUpdateInsert)
    {
        TTabletFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-update-insert";
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
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-aggregate-insert";
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

    Y_UNIT_TEST(TestVersionedLookup)
    {
        TTabletFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-versioned-lookup";
        CreateTestTable(client, tablePath);
        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        const TString values[] = {"one0", "one1", "one2"};

        client->InsertRows(tablePath, {TNode()("key", 1)("value", values[0])});
        client->InsertRows(tablePath, {TNode()("key", 1)("value", values[1])});

        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 1)}, TLookupRowsOptions().Versioned(true))[0];
            auto writeTs = result.GetAttributes().AsMap().at("write_timestamps");
            auto deleteTs = result.GetAttributes().AsMap().at("delete_timestamps");
            UNIT_ASSERT_VALUES_EQUAL(writeTs.Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(deleteTs.Size(), 0);
            int valueIdx = 1;
            for (const auto& value : result["value"].AsList()) {
                UNIT_ASSERT_VALUES_EQUAL(values[valueIdx], value.AsString());
                --valueIdx;
            }
        }

        client->DeleteRows(tablePath, {TNode()("key", 1)});
        client->InsertRows(tablePath, {TNode()("key", 1)("value", values[2])});

        {
            auto result = client->LookupRows(tablePath, {TNode()("key", 1)}, TLookupRowsOptions().Versioned(true))[0];
            auto writeTs = result.GetAttributes().AsMap().at("write_timestamps");
            auto deleteTs = result.GetAttributes().AsMap().at("delete_timestamps");
            UNIT_ASSERT_VALUES_EQUAL(writeTs.Size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(deleteTs.Size(), 1);
            int valueIdx = 2;
            for (const auto& value : result["value"].AsList()) {
                UNIT_ASSERT_VALUES_EQUAL(values[valueIdx], value.AsString());
                --valueIdx;
            }
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestGetTabletInfos)
    {
        TTabletFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-GetTabletInfos";
        CreateTestTable(client, tablePath);

        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        auto infos = client->GetTabletInfos(tablePath, {0});
        auto ts = client->GenerateTimestamp();

        UNIT_ASSERT_VALUES_EQUAL(infos.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(infos[0].TotalRowCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(infos[0].TrimmedRowCount, 0);
        UNIT_ASSERT_LE(infos[0].BarrierTimestamp, ts);

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }

    Y_UNIT_TEST(TestInsertRowsEarlyError)
    {
        TTabletFixture fixture;
        auto client = fixture.GetClient();

        auto workingDir = fixture.GetWorkingDir();
        const TString tablePath = workingDir + "/test-InsertRowsEarlyError";
        CreateTestTable(client, tablePath);

        client->MountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_MOUNTED, WaitTabletsOptions);

        auto rows = TVector(1024 * 1024, TNode()("bad_row", 100500));

        try {
            client->InsertRows(tablePath, rows);
            UNIT_FAIL("Expected exception to be thrown");
        } catch (const TErrorResponse& e) {
            UNIT_ASSERT(e.GetError().ContainsErrorCode(NYT::NClusterErrorCodes::NTableClient::SchemaViolation));
        }

        client->UnmountTable(tablePath);
        WaitForTabletsState(client, tablePath, TS_UNMOUNTED, WaitTabletsOptions);
    }
}
