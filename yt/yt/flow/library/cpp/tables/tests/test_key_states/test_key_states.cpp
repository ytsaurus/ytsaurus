#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/key_states.h>
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

class TTestKeyStates
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
        WaitUntilTableMounted(YPathJoin(GetPipelinePath(), "states"));
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
        return "//pipeline_key_states";
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
        const NTables::TKeyStatesPtr& table,
        const NTables::TKeyStates::TTableKey& key,
        const NYsonSerializer::TStateMutation& mutation)
    {
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        table->Write(tx, key, mutation);
        WaitFor(tx->Commit()).ThrowOnError();
    }

    // Commits an Erase() call inside a dynamic table transaction.
    void EraseStates(
        const NTables::TKeyStatesPtr& table,
        const THashSet<NTables::TKeyStates::TTableKey>& keys)
    {
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        table->Erase(tx, keys);
        WaitFor(tx->Commit()).ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_W(TTestKeyStates, WriteAndLookup)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TKeyStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const NTables::TKeyStates::TTableKey tableKey{
        .ComputationId = TComputationId("comp-1"),
        .Key = MakeKey(ToString("key-1")),
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

TEST_W(TTestKeyStates, LookupMissing)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TKeyStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const NTables::TKeyStates::TTableKey tableKey{
        .ComputationId = TComputationId("comp-missing"),
        .Key = MakeKey(ToString("key-missing")),
        .Name = "state-name",
    };

    // Lookup of a key that was never written — returns a non-null but empty state.
    auto result = WaitFor(table->Lookup(tableKey)).ValueOrThrow();
    ASSERT_TRUE(result != nullptr);
}

TEST_W(TTestKeyStates, WriteAndErase)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TKeyStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const NTables::TKeyStates::TTableKey tableKey{
        .ComputationId = TComputationId("comp-erase"),
        .Key = MakeKey(ToString("key-erase")),
        .Name = "state-name",
    };

    // Write a state entry.
    WriteState(table, tableKey, MakeUpdateMutation());

    // Verify it appears in ListAll.
    auto before = WaitFor(table->ListAll(NTables::TKeyStates::TTableKeyFilter{
            .ComputationId = TComputationId("comp-erase")}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(before), 1);

    // Erase it.
    EraseStates(table, {tableKey});

    // After erase — no longer listed.
    auto after = WaitFor(table->ListAll(NTables::TKeyStates::TTableKeyFilter{
            .ComputationId = TComputationId("comp-erase")}))
        .ValueOrThrow();
    EXPECT_TRUE(after.empty());
}

// Verifies that Write() with TEraseMutation deletes the row from the table,
// exercising the TEraseMutation branch in TKeyStates::Write().
TEST_W(TTestKeyStates, WriteAndEraseViaMutation)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TKeyStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const NTables::TKeyStates::TTableKey tableKey{
        .ComputationId = TComputationId("comp-erase-mutation"),
        .Key = MakeKey(ToString("key-erase-mutation")),
        .Name = "state-name",
    };

    // Write a state entry first.
    WriteState(table, tableKey, MakeUpdateMutation());

    // Verify it appears in ListAll.
    auto before = WaitFor(table->ListAll(NTables::TKeyStates::TTableKeyFilter{
            .ComputationId = TComputationId("comp-erase-mutation")}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(before), 1);

    // Delete via TEraseMutation passed to Write().
    WriteState(table, tableKey, NYsonSerializer::TEraseMutation{});

    // After erase mutation — no longer listed.
    auto after = WaitFor(table->ListAll(NTables::TKeyStates::TTableKeyFilter{
            .ComputationId = TComputationId("comp-erase-mutation")}))
        .ValueOrThrow();
    EXPECT_TRUE(after.empty());
}

TEST_W(TTestKeyStates, ListAll)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TKeyStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const TComputationId computationId("comp-list");

    NTables::TKeyStates::TTableKey key1{
        .ComputationId = computationId,
        .Key = MakeKey(ToString("key-a")),
        .Name = "state-name",
    };
    NTables::TKeyStates::TTableKey key2{
        .ComputationId = computationId,
        .Key = MakeKey(ToString("key-b")),
        .Name = "state-name",
    };

    WriteState(table, key1, MakeUpdateMutation());
    WriteState(table, key2, MakeUpdateMutation());

    auto result = WaitFor(table->ListAll(NTables::TKeyStates::TTableKeyFilter{
            .ComputationId = computationId}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(result), 2);
}

TEST_W(TTestKeyStates, ListWithFilter)
{
    auto tablesContext = PrepareTablesContext();
    auto table = New<NTables::TKeyStates>(tablesContext, New<TDynamicTableRequestSpec>());

    const TComputationId comp1("comp-filter-x");
    const TComputationId comp2("comp-filter-y");

    NTables::TKeyStates::TTableKey key1{
        .ComputationId = comp1,
        .Key = MakeKey(ToString("key-1")),
        .Name = "state-name",
    };
    NTables::TKeyStates::TTableKey key2{
        .ComputationId = comp2,
        .Key = MakeKey(ToString("key-1")),
        .Name = "state-name",
    };

    WriteState(table, key1, MakeUpdateMutation());
    WriteState(table, key2, MakeUpdateMutation());

    // Filter by comp1 — only key1.
    auto result1 = WaitFor(table->ListAll(NTables::TKeyStates::TTableKeyFilter{
            .ComputationId = comp1}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(result1), 1);
    EXPECT_EQ(result1[0].ComputationId, comp1);

    // Filter by comp2 — only key2.
    auto result2 = WaitFor(table->ListAll(NTables::TKeyStates::TTableKeyFilter{
            .ComputationId = comp2}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(result2), 1);
    EXPECT_EQ(result2[0].ComputationId, comp2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
