#include <yt/yt/flow/library/cpp/computation/job_state/state_manager.h>
#include <yt/yt/flow/library/cpp/computation/job_state/state_providers.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/key_states.h>
#include <yt/yt/flow/library/cpp/tables/unittests/mock/partition_states.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/serializer/state.h>
#include <yt/yt/flow/library/cpp/tables/state.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NConcurrency;
using namespace NYT::NYson;
using namespace NYT::NFlow::NYsonSerializer;

////////////////////////////////////////////////////////////////////////////////

TDynamicStateFormatSpecPtr MakeFormat()
{
    auto format = New<TDynamicStateFormatSpec>();
    format->Compress = true;
    // Always take the recode branch in #TRemoteState::FlushMutation().
    format->RecodeProbability = 1.0;
    // A non-default codec, so the format column is actually materialized.
    format->Compression = NCompression::ECodec::Lz4;
    return format;
}

TYsonString MakeValue(int value)
{
    return ConvertToYsonString(THashMap<std::string, int>{{"value", value}});
}

////////////////////////////////////////////////////////////////////////////////

// A state that no computation touched during the epoch must not be rewritten just
// because the recode roll succeeded.
TEST(TRemoteStateTest, RecodeDoesNotRewriteUnchangedState)
{
    auto stateSchema = GetYsonStateSchema<NTables::TInternalState>();
    auto format = MakeFormat();

    // First epoch: the state is written for the first time.
    auto tableState = New<TState>(stateSchema);
    auto remoteState = New<TRemoteState>(tableState, format);
    remoteState->Set(MakeValue(42));
    auto mutation = remoteState->FlushMutation();
    ASSERT_TRUE(std::get_if<TUpdateMutation>(&mutation));

    auto row = tableState->GetTableRow();
    ASSERT_TRUE(row.has_value());

    // Second epoch: the state is looked up again and flushed back unchanged, the way
    // a computation that materialized its state but did not modify it would.
    auto reloadedTableState = New<TState>(stateSchema);
    reloadedTableState->Init(row);
    auto reloadedRemoteState = New<TRemoteState>(reloadedTableState, format);
    EXPECT_EQ(reloadedRemoteState->Get().ToString(), MakeValue(42).ToString());

    reloadedRemoteState->Set(MakeValue(42));
    auto reloadedMutation = reloadedRemoteState->FlushMutation();
    EXPECT_TRUE(std::get_if<TEmptyMutation>(&reloadedMutation));

    // Third epoch: the state was never materialized at all.
    auto untouchedTableState = New<TState>(stateSchema);
    untouchedTableState->Init(row);
    auto untouchedRemoteState = New<TRemoteState>(untouchedTableState, format);
    auto untouchedMutation = untouchedRemoteState->FlushMutation();
    EXPECT_TRUE(std::get_if<TEmptyMutation>(&untouchedMutation));
}

// An empty state whose row was never written must not produce an erase every epoch.
TEST(TRemoteStateTest, RecodeDoesNotEraseAbsentEmptyState)
{
    auto stateSchema = GetYsonStateSchema<NTables::TInternalState>();

    auto tableState = New<TState>(stateSchema);
    auto remoteState = New<TRemoteState>(tableState, MakeFormat());
    ASSERT_TRUE(remoteState->IsEmpty());

    auto mutation = remoteState->FlushMutation();
    EXPECT_TRUE(std::get_if<TEmptyMutation>(&mutation));
}

// A changed format still migrates the state to the new codec.
TEST(TRemoteStateTest, ChangedFormatRewritesState)
{
    auto stateSchema = GetYsonStateSchema<NTables::TInternalState>();

    auto tableState = New<TState>(stateSchema);
    auto remoteState = New<TRemoteState>(tableState, MakeFormat());
    remoteState->Set(MakeValue(42));
    auto mutation = remoteState->FlushMutation();
    ASSERT_TRUE(std::get_if<TUpdateMutation>(&mutation));

    auto row = tableState->GetTableRow();
    ASSERT_TRUE(row.has_value());

    auto newFormat = MakeFormat();
    newFormat->Compression = NCompression::ECodec::Zstd_6;

    auto reloadedTableState = New<TState>(stateSchema);
    reloadedTableState->Init(row);
    auto reloadedRemoteState = New<TRemoteState>(reloadedTableState, newFormat);
    auto reloadedMutation = reloadedRemoteState->FlushMutation();
    ASSERT_TRUE(std::get_if<TUpdateMutation>(&reloadedMutation));
    EXPECT_EQ(reloadedTableState->GetFormat()->Compression, NCompression::ECodec::Zstd_6);
    EXPECT_EQ(reloadedRemoteState->Get().ToString(), MakeValue(42).ToString());
}

////////////////////////////////////////////////////////////////////////////////

// End-to-end counterpart of the tests above: drives whole state-manager epochs against
// the in-memory state tables and counts the rows they actually write. This is the shape
// of the production symptom — every key rewritten on every epoch.
class TStateWriteAmplificationTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId = TComputationId("test-computation");
    const TPartitionId PartitionId = TPartitionId(TGuid::Create());

    NTables::TInMemoryKeyStatesPtr KeyStates = New<NTables::TInMemoryKeyStates>();
    NTables::TInMemoryPartitionStatesPtr PartitionStates = New<NTables::TInMemoryPartitionStates>();

    TJobStateManagerContextPtr MakeManagerContext()
    {
        auto context = New<TJobStateManagerContext>();
        context->ComputationId = ComputationId;
        context->PartitionId = PartitionId;
        context->Logger = NLogging::TLogger("Test");
        context->Profiler = NProfiling::TProfiler();
        context->KeyStates = KeyStates;
        context->PartitionStates = PartitionStates;
        return context;
    }

    // Every epoch gets a freshly allocated spec, exactly as a reconfigure does.
    // #TRemoteState::TrySetFormat() compares spec pointers, so this resets
    // |FormatSynced_| each epoch and re-runs #TState::SetFormat() — the trigger that
    // made the always-true format comparison rewrite every state row.
    static TDynamicJobStateManagerContextPtr MakeDynamicManagerContext()
    {
        auto dynamicContext = New<TDynamicJobStateManagerContext>();
        dynamicContext->StateManager = New<TDynamicStateManagerSpec>();
        dynamicContext->StateManager->Format->Compress = true;
        dynamicContext->StateManager->Format->RecodeProbability = 1.0;
        return dynamicContext;
    }

    TJobStateManagerPtr MakeManager()
    {
        return New<TJobStateManager>(MakeManagerContext(), MakeDynamicManagerContext());
    }
};

TEST_F(TStateWriteAmplificationTest, UnchangedKeyStateIsWrittenOnce)
{
    auto key = MakeKey<ui64>(1);

    {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        *client = 7;
        manager->Sync(/*transaction*/ nullptr);
    }
    EXPECT_EQ(KeyStates->GetWrittenKeyCount(), 1);

    // Subsequent epochs load the state and leave it alone.
    for (int epoch = 0; epoch < 5; ++epoch) {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        EXPECT_EQ(*client, 7);
        manager->Sync(/*transaction*/ nullptr);
    }
    EXPECT_EQ(KeyStates->GetWrittenKeyCount(), 1);

    // A real change is still written.
    {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        *client = 8;
        manager->Sync(/*transaction*/ nullptr);
    }
    EXPECT_EQ(KeyStates->GetWrittenKeyCount(), 2);

    {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        EXPECT_EQ(*client, 8);
    }
}

TEST_F(TStateWriteAmplificationTest, UnchangedPartitionStateIsWrittenOnce)
{
    {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsPartition()->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        *client = 7;
        manager->Sync(/*transaction*/ nullptr);
    }
    EXPECT_EQ(PartitionStates->GetWrittenKeyCount(), 1);

    for (int epoch = 0; epoch < 5; ++epoch) {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsPartition()->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        EXPECT_EQ(*client, 7);
        manager->Sync(/*transaction*/ nullptr);
    }
    EXPECT_EQ(PartitionStates->GetWrittenKeyCount(), 1);
}

TEST_F(TStateWriteAmplificationTest, EmptyStateIsNeverWritten)
{
    auto key = MakeKey<ui64>(1);

    for (int epoch = 0; epoch < 5; ++epoch) {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        EXPECT_TRUE(client.IsEmpty());
        manager->Sync(/*transaction*/ nullptr);
    }
    EXPECT_EQ(KeyStates->GetWrittenKeyCount(), 0);
}

TEST_F(TStateWriteAmplificationTest, ClearedStateIsErasedOnce)
{
    auto key = MakeKey<ui64>(1);

    {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        *client = 7;
        manager->Sync(/*transaction*/ nullptr);
    }
    EXPECT_EQ(KeyStates->GetWrittenKeyCount(), 1);

    {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        client.Clear();
        manager->Sync(/*transaction*/ nullptr);
    }
    EXPECT_EQ(KeyStates->GetWrittenKeyCount(), 2);

    // The row is already gone; further epochs must not keep erasing it.
    for (int epoch = 0; epoch < 3; ++epoch) {
        auto manager = MakeManager();
        auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        EXPECT_TRUE(client.IsEmpty());
        manager->Sync(/*transaction*/ nullptr);
    }
    EXPECT_EQ(KeyStates->GetWrittenKeyCount(), 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
