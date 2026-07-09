#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/partition_states.h>
#include <yt/yt/flow/library/cpp/tables/state.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/flow/lib/serializer/public.h>
#include <yt/yt/flow/lib/serializer/state.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/cache/rpc.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYPath;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");

////////////////////////////////////////////////////////////////////////////////

class TTestPartitionStates
    : public ::testing::Test
{
protected:
    NApi::IClientPtr Client_;

    void SetUp() override
    {
        // Connect through the RPC proxy using YT_PROXY (exported by the Flow recipe). Going
        // through the proxy means the pipeline node type handler that bootstraps a pipeline's
        // child tables runs from the package binaries, not from the (possibly older) code linked
        // into this test binary.
        Client_ = NClient::NCache::CreateClient();

        WaitFor(Client_->CreateNode(GetPipelinePath(), EObjectType::Pipeline))
            .ThrowOnError();
        WaitUntilTableMounted(YPathJoin(GetPipelinePath(), "partition_states"));
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetPipelinePath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iterationCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
    }

    static TYPath GetPipelinePath()
    {
        return "//pipeline_partition_states";
    }

    void WaitUntilTableMounted(const TYPath& tablePath)
    {
        WaitForPredicate(
            [&] {
                auto state = WaitFor(Client_->GetNode(tablePath + "/@tablet_state")).ValueOrThrow();
                return NYTree::ConvertTo<std::string>(state) == "mounted";
            },
            /*iterationCount*/ 100,
            /*period*/ TDuration::MilliSeconds(500));
    }

    NTables::TContextPtr PrepareTablesContext()
    {
        auto context = New<NTables::TContext>();
        context->Client = Client_;
        context->PipelinePath = GetPipelinePath();
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>(
            CreateNamedUnlimitedThroughputThrottler("test", NProfiling::TProfiler()),
            Logger(),
            NProfiling::TProfiler());
        context->Logger = Logger();
        context->Profiler = NProfiling::TProfiler();
        return context;
    }

    // Builds a minimal TUpdateMutation with null state columns (required by YT dynamic tables).
    // Builds a minimal TUpdateMutation with a non-empty state (required by YT dynamic tables).
    NYsonSerializer::TUpdateMutation MakeUpdateMutation()
    {
        auto schema = NYsonSerializer::GetYsonStateSchema<NTables::TInternalState>();
        auto state = New<NYsonSerializer::TState>(schema);
        state->Init();
        // Set a non-empty State field so FlushMutation returns TUpdateMutation
        // rather than TEmptyMutation (which would be skipped by Write()).
        auto internalState = New<NTables::TInternalState>();
        internalState->State = NYson::ConvertToYsonString(TString("abracadabra"));
        state->SetValue(internalState);
        auto mutation = state->FlushMutation();
        YT_VERIFY(std::holds_alternative<NYsonSerializer::TUpdateMutation>(mutation));
        return std::get<NYsonSerializer::TUpdateMutation>(std::move(mutation));
    }

    // Commits a Write() call inside a dynamic table transaction.
    void WriteState(
        const NTables::TPartitionStatesPtr& table,
        const NTables::TPartitionStates::TTableKey& key,
        const NYsonSerializer::TStateMutation& mutation)
    {
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        table->Write(tx, key, mutation);
        WaitFor(tx->Commit()).ThrowOnError();
    }

    // Commits an Erase() call inside a dynamic table transaction.
    void EraseStates(
        const NTables::TPartitionStatesPtr& table,
        const THashSet<NTables::TPartitionStates::TTableKey>& keys)
    {
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        table->Erase(tx, keys);
        WaitFor(tx->Commit()).ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_W(TTestPartitionStates, WriteAndLookup)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TPartitionStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const NTables::TPartitionStates::TTableKey tableKey{
        .PartitionId = TPartitionId(TGuid::Create()),
        .Name = "state-name",
    };

    // Before write — state exists but is empty.
    auto before = WaitFor(table->Lookup(tableKey)).ValueOrThrow();
    ASSERT_TRUE(before != nullptr);

    // Write a state entry.
    WriteState(table, tableKey, MakeUpdateMutation());

    // After write — state is still accessible.
    auto after = WaitFor(table->Lookup(tableKey)).ValueOrThrow();
    ASSERT_TRUE(after != nullptr);
}

TEST_W(TTestPartitionStates, WriteAndErase)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TPartitionStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const TPartitionId partitionId(TGuid::Create());
    const NTables::TPartitionStates::TTableKey tableKey{
        .PartitionId = partitionId,
        .Name = "state-name",
    };

    // Write a state entry.
    WriteState(table, tableKey, MakeUpdateMutation());

    // Verify it appears in ListAll.
    auto before = WaitFor(table->ListAll(NTables::TPartitionStates::TTableKeyFilter{
            .PartitionId = partitionId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(before), 1);

    // Erase it.
    EraseStates(table, {tableKey});

    // After erase — no longer listed.
    auto after = WaitFor(table->ListAll(NTables::TPartitionStates::TTableKeyFilter{
            .PartitionId = partitionId}))
        .ValueOrThrow();
    EXPECT_TRUE(after.empty());
}

// Verifies that Write() with TEraseMutation deletes the row from the table,
// exercising the TEraseMutation branch in TPartitionStates::Write().
TEST_W(TTestPartitionStates, WriteAndEraseViaMutation)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TPartitionStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const TPartitionId partitionId(TGuid::Create());
    const NTables::TPartitionStates::TTableKey tableKey{
        .PartitionId = partitionId,
        .Name = "state-erase-mutation",
    };

    // Write a state entry first.
    WriteState(table, tableKey, MakeUpdateMutation());

    // Verify it appears in ListAll.
    auto before = WaitFor(table->ListAll(NTables::TPartitionStates::TTableKeyFilter{
            .PartitionId = partitionId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(before), 1);

    // Delete via TEraseMutation passed to Write().
    WriteState(table, tableKey, NYsonSerializer::TEraseMutation{});

    // After erase mutation — no longer listed.
    auto after = WaitFor(table->ListAll(NTables::TPartitionStates::TTableKeyFilter{
            .PartitionId = partitionId}))
        .ValueOrThrow();
    EXPECT_TRUE(after.empty());
}

TEST_W(TTestPartitionStates, ListAll)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TPartitionStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const TPartitionId partitionId(TGuid::Create());

    NTables::TPartitionStates::TTableKey key1{
        .PartitionId = partitionId,
        .Name = "state-a",
    };
    NTables::TPartitionStates::TTableKey key2{
        .PartitionId = partitionId,
        .Name = "state-b",
    };

    WriteState(table, key1, MakeUpdateMutation());
    WriteState(table, key2, MakeUpdateMutation());

    auto result = WaitFor(table->ListAll(NTables::TPartitionStates::TTableKeyFilter{
            .PartitionId = partitionId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(result), 2);
}

TEST_W(TTestPartitionStates, ListWithNameFilter)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TPartitionStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const TPartitionId partitionId(TGuid::Create());

    NTables::TPartitionStates::TTableKey key1{
        .PartitionId = partitionId,
        .Name = "state-x",
    };
    NTables::TPartitionStates::TTableKey key2{
        .PartitionId = partitionId,
        .Name = "state-y",
    };

    WriteState(table, key1, MakeUpdateMutation());
    WriteState(table, key2, MakeUpdateMutation());

    // Filter by name — only key1.
    auto result = WaitFor(table->ListAll(NTables::TPartitionStates::TTableKeyFilter{
            .PartitionId = partitionId,
            .Name = "state-x"}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(result), 1);
    EXPECT_EQ(result[0].Name, "state-x");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
