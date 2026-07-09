#include <yt/yt/flow/library/cpp/computation/job_state/state_manager.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/key_states.h>
#include <yt/yt/flow/library/cpp/tables/unittests/mock/partition_states.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TStateClientTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId = TComputationId("test-computation");
    const TPartitionId PartitionId = TPartitionId(TGuid::Create());

    // Build a fresh TJobStateManagerContext with new mock tables and no cache.
    TJobStateManagerContextPtr MakeManagerContext()
    {
        auto context = New<TJobStateManagerContext>();
        context->ComputationId = ComputationId;
        context->PartitionId = PartitionId;
        context->Logger = NLogging::TLogger("Test");
        context->Profiler = NProfiling::TProfiler();
        context->KeyStates = New<NTables::TInMemoryKeyStates>();
        context->PartitionStates = New<NTables::TInMemoryPartitionStates>();
        // StateCache is null — cache disabled.
        return context;
    }

    // Build a fresh TJobStateManagerContext with new mock tables and an enabled cache.
    // Both manager epochs must share the same context instance for cache hits to work.
    TJobStateManagerContextPtr MakeManagerContextWithCache()
    {
        auto managerContext = MakeManagerContext();
        auto stateCache = New<TStateCache>(New<TDynamicStateCacheSpec>(), NProfiling::TProfiler{});
        managerContext->StateCache = stateCache->WithJob(TJobId(TGuid::Create()), NProfiling::TProfiler{});
        return managerContext;
    }

    static TDynamicJobStateManagerContextPtr MakeDynamicManagerContext()
    {
        auto dynamicContext = New<TDynamicJobStateManagerContext>();
        dynamicContext->StateManager = New<TDynamicStateManagerSpec>();
        return dynamicContext;
    }

    static TJobStateManagerPtr MakeManager(TJobStateManagerContextPtr context)
    {
        return New<TJobStateManager>(std::move(context), MakeDynamicManagerContext());
    }

    static IInputContextPtr MakeInputContext(std::initializer_list<TKey> keys)
    {
        std::vector<TInputMessageConstPtr> messages;
        messages.reserve(keys.size());
        auto schema = New<NTableClient::TTableSchema>();
        i64 idx = 0;
        for (const auto& key : keys) {
            TMessageBuilder builder(TStreamId("test"), schema);
            builder.SetMessageId(TMessageId(Format("test-%v", idx++)));
            builder.SetSystemTimestamp(TSystemTimestamp(1));
            builder.SetAlignmentTimestamp(TSystemTimestamp(1));
            builder.SetEventTimestamp(TSystemTimestamp(1));
            messages.push_back(New<TInputMessage>(builder.Finish(), key));
        }
        return New<TInputContext>(messages, std::vector<TInputTimerConstPtr>{});
    }
};

////////////////////////////////////////////////////////////////////////////////
// Partition State — basic lifecycle

TEST_F(TStateClientTest, PartitionStateInitialIsEmpty)
{
    auto managerContext = MakeManagerContext();
    auto manager = MakeManager(managerContext);
    auto client = WaitFor(manager->CreateContext()->AsPartition()->CreateMutableStateClient<i64>("counter"))
        .ValueOrThrow();

    EXPECT_TRUE(client.IsEmpty());
    EXPECT_EQ(*client, 0);
}

TEST_F(TStateClientTest, PartitionStateBasic)
{
    // Use one shared context across epochs to simulate persistence via shared tables.
    // Verify: initial=0, save, reload, update, reload, clear, reload=0.
    auto managerContext = MakeManagerContext();

    // Initial load: two names, both zero.
    {
        auto manager = MakeManager(managerContext);
        auto partitionCtx = manager->CreateContext()->AsPartition();
        auto state_a = WaitFor(partitionCtx->CreateMutableStateClient<i64>("state_a")).ValueOrThrow();
        auto state_b = WaitFor(partitionCtx->CreateMutableStateClient<i64>("state_b")).ValueOrThrow();
        EXPECT_EQ(*state_a, 0);
        EXPECT_EQ(*state_b, 0);

        // Modify and save.
        *state_a = 7;
        *state_b = 42;
        manager->Sync(/*transaction*/ nullptr);
    }

    // Reload and verify.
    {
        auto manager = MakeManager(managerContext);
        auto partitionCtx = manager->CreateContext()->AsPartition();
        auto state_a = WaitFor(partitionCtx->CreateMutableStateClient<i64>("state_a")).ValueOrThrow();
        auto state_b = WaitFor(partitionCtx->CreateMutableStateClient<i64>("state_b")).ValueOrThrow();
        EXPECT_EQ(*state_a, 7);
        EXPECT_EQ(*state_b, 42);

        // Modify to different value.
        *state_a = 100;
        manager->Sync(/*transaction*/ nullptr);
    }

    // Verify updated value.
    {
        auto manager = MakeManager(managerContext);
        auto partitionCtx = manager->CreateContext()->AsPartition();
        auto state_a = WaitFor(partitionCtx->CreateMutableStateClient<i64>("state_a")).ValueOrThrow();
        EXPECT_EQ(*state_a, 100);

        // Clear and sync.
        state_a.Clear();
        manager->Sync(/*transaction*/ nullptr);
    }

    // After clear, should be empty/default.
    {
        auto manager = MakeManager(managerContext);
        auto partitionCtx = manager->CreateContext()->AsPartition();
        auto state_a = WaitFor(partitionCtx->CreateMutableStateClient<i64>("state_a")).ValueOrThrow();
        EXPECT_TRUE(state_a.IsEmpty());
        EXPECT_EQ(*state_a, 0);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Key State — basic lifecycle

TEST_F(TStateClientTest, KeyStateInitialIsEmpty)
{
    auto managerContext = MakeManagerContext();
    auto manager = MakeManager(managerContext);
    auto key = MakeKey<ui64>(1);
    auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
        .ValueOrThrow();

    EXPECT_TRUE(client.IsEmpty());
    EXPECT_EQ(*client, 0);
}

TEST_F(TStateClientTest, KeyStateBasic)
{
    auto managerContext = MakeManagerContext();
    auto key1 = MakeKey<ui64>(1);
    auto key2 = MakeKey<ui64>(2);

    // Initial load: two keys, both zero.
    {
        auto manager = MakeManager(managerContext);
        auto ctx = manager->CreateContext();
        auto state_a = WaitFor(ctx->AsKey(key1)->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        auto state_b = WaitFor(ctx->AsKey(key2)->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        EXPECT_EQ(*state_a, 0);
        EXPECT_EQ(*state_b, 0);

        // Modify and save.
        *state_a = 100;
        *state_b = 200;
        manager->Sync(/*transaction*/ nullptr);
    }

    // Reload and verify.
    {
        auto manager = MakeManager(managerContext);
        auto ctx = manager->CreateContext();
        auto state_a = WaitFor(ctx->AsKey(key1)->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        auto state_b = WaitFor(ctx->AsKey(key2)->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        EXPECT_EQ(*state_a, 100);
        EXPECT_EQ(*state_b, 200);

        // Modify to different value.
        *state_a = 999;
        manager->Sync(/*transaction*/ nullptr);
    }

    // Verify updated value.
    {
        auto manager = MakeManager(managerContext);
        auto ctx = manager->CreateContext();
        auto state_a = WaitFor(ctx->AsKey(key1)->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        EXPECT_EQ(*state_a, 999);

        // Clear and sync.
        state_a.Clear();
        manager->Sync(/*transaction*/ nullptr);
    }

    // After clear, should be empty/default.
    {
        auto manager = MakeManager(managerContext);
        auto ctx = manager->CreateContext();
        auto state_a = WaitFor(ctx->AsKey(key1)->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        EXPECT_TRUE(state_a.IsEmpty());
        EXPECT_EQ(*state_a, 0);
    }
}

////////////////////////////////////////////////////////////////////////////////
// KeyClient (multi-key) — basic lifecycle

TEST_F(TStateClientTest, KeyClientInitialIsEmpty)
{
    auto managerContext = MakeManagerContext();
    auto manager = MakeManager(managerContext);
    auto jobCtx = manager->CreateContext();

    auto keyClient = WaitFor(jobCtx->CreateMutableStateKeyClient<i64>("counter"))
        .ValueOrThrow();

    auto key = MakeKey<ui64>(99);
    WaitFor(manager->PreloadKeyStates(MakeInputContext({key}))).ThrowOnError();

    EXPECT_EQ(*keyClient.GetState(key), 0);
}

TEST_F(TStateClientTest, KeyClientGetStateByMessage)
{
    auto managerContext = MakeManagerContext();
    auto manager = MakeManager(managerContext);
    auto jobCtx = manager->CreateContext();

    auto keyClient = WaitFor(jobCtx->CreateMutableStateKeyClient<i64>("counter"))
        .ValueOrThrow();

    auto key = MakeKey<ui64>(7);
    WaitFor(manager->PreloadKeyStates(MakeInputContext({key}))).ThrowOnError();
    // The client-side PreloadKeyStates(inputContext) overload must at least be callable.
    WaitFor(keyClient.PreloadKeyStates(MakeInputContext({key}))).ThrowOnError();

    *keyClient.GetState(key) = 42;

    // GetState(message) resolves the key off the message and reaches the same state.
    auto schema = New<NTableClient::TTableSchema>();
    TMessageBuilder builder(TStreamId("test"), schema);
    builder.SetMessageId(TMessageId("msg"));
    builder.SetSystemTimestamp(TSystemTimestamp(1));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1));
    builder.SetEventTimestamp(TSystemTimestamp(1));
    auto message = New<TInputMessage>(builder.Finish(), key);

    EXPECT_EQ(*keyClient.GetState(message), 42);
}

TEST_F(TStateClientTest, KeyClientBasic)
{
    auto managerContext = MakeManagerContext();
    auto key1 = MakeKey<ui64>(1);
    auto key2 = MakeKey<ui64>(2);

    // Initial load: two keys, both zero.
    {
        auto manager = MakeManager(managerContext);
        auto keyClient = WaitFor(manager->CreateContext()->CreateMutableStateKeyClient<i64>("counter"))
            .ValueOrThrow();
        WaitFor(manager->PreloadKeyStates(MakeInputContext({key1, key2}))).ThrowOnError();
        EXPECT_EQ(*keyClient.GetState(key1), 0);
        EXPECT_EQ(*keyClient.GetState(key2), 0);

        // Modify and save.
        *keyClient.GetState(key1) = 111;
        *keyClient.GetState(key2) = 222;
        manager->Sync(/*transaction*/ nullptr);
    }

    // Reload and verify.
    {
        auto manager = MakeManager(managerContext);
        auto keyClient = WaitFor(manager->CreateContext()->CreateMutableStateKeyClient<i64>("counter"))
            .ValueOrThrow();
        WaitFor(manager->PreloadKeyStates(MakeInputContext({key1, key2}))).ThrowOnError();
        EXPECT_EQ(*keyClient.GetState(key1), 111);
        EXPECT_EQ(*keyClient.GetState(key2), 222);

        // Modify to different value.
        *keyClient.GetState(key1) = 999;
        manager->Sync(/*transaction*/ nullptr);
    }

    // Verify updated value.
    {
        auto manager = MakeManager(managerContext);
        auto keyClient = WaitFor(manager->CreateContext()->CreateMutableStateKeyClient<i64>("counter"))
            .ValueOrThrow();
        WaitFor(manager->PreloadKeyStates(MakeInputContext({key1, key2}))).ThrowOnError();
        EXPECT_EQ(*keyClient.GetState(key1), 999);
        EXPECT_EQ(*keyClient.GetState(key2), 222);
    }
}

TEST_F(TStateClientTest, KeyClientCacheReducesLookups)
{
    // Verify that after Preload+Sync, the second epoch loads from cache
    // and does NOT issue a new Lookup to the table for already-cached keys.
    // Also verify that WITHOUT cache there are 2 lookups.
    auto key1 = MakeKey<ui64>(1);
    auto key2 = MakeKey<ui64>(2);

    // First verify: without cache, two epochs = 2 lookup calls, 4 loaded keys total.
    {
        auto managerContext = MakeManagerContext();
        auto keyStates = DynamicPointerCast<NTables::TInMemoryKeyStates>(managerContext->KeyStates);

        // Epoch 1 without cache.
        {
            auto manager = MakeManager(managerContext);
            auto keyClient = WaitFor(manager->CreateContext()->CreateMutableStateKeyClient<i64>("counter"))
                .ValueOrThrow();
            WaitFor(manager->PreloadKeyStates(MakeInputContext({key1, key2}))).ThrowOnError();
        }
        // Epoch 2 without cache.
        {
            auto manager = MakeManager(managerContext);
            auto keyClient = WaitFor(manager->CreateContext()->CreateMutableStateKeyClient<i64>("counter"))
                .ValueOrThrow();
            WaitFor(manager->PreloadKeyStates(MakeInputContext({key1, key2}))).ThrowOnError();
        }
        EXPECT_EQ(keyStates->GetLookupCount(), 2);
        EXPECT_EQ(keyStates->GetLoadedKeyCount(), 4);
    }

    // Both epochs share the same context (same tables + same cache) so cache entries
    // survive across manager instances.
    {
        auto managerContext = MakeManagerContextWithCache();
        auto keyStates = DynamicPointerCast<NTables::TInMemoryKeyStates>(managerContext->KeyStates);

        // Epoch 1: Preload, modify, Sync — populates cache.
        {
            auto manager = MakeManager(managerContext);
            auto keyClient = WaitFor(manager->CreateContext()->CreateMutableStateKeyClient<i64>("counter"))
                .ValueOrThrow();

            WaitFor(manager->PreloadKeyStates(MakeInputContext({key1, key2}))).ThrowOnError();
            // First epoch: 1 Lookup call (batch of 2 keys).
            EXPECT_EQ(keyStates->GetLookupCount(), 1);
            EXPECT_EQ(keyStates->GetLoadedKeyCount(), 2);

            *keyClient.GetState(key1) = 42;
            *keyClient.GetState(key2) = 43;
            manager->Sync(/*transaction*/ nullptr);
            // After Sync, KeyClient clears its in-memory state and inserts into cache.
        }

        // Epoch 2: same keys — should be served from cache, no new Lookup.
        {
            auto manager = MakeManager(managerContext);
            auto keyClient = WaitFor(manager->CreateContext()->CreateMutableStateKeyClient<i64>("counter"))
                .ValueOrThrow();

            WaitFor(manager->PreloadKeyStates(MakeInputContext({key1, key2}))).ThrowOnError();
            // Cache hit: LookupCount must still be 1 (no new table lookup).
            EXPECT_EQ(keyStates->GetLookupCount(), 1);
            EXPECT_EQ(keyStates->GetLoadedKeyCount(), 2);

            EXPECT_EQ(*keyClient.GetState(key1), 42);
            EXPECT_EQ(*keyClient.GetState(key2), 43);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Lifecycle — client must be alive during Sync

TEST_F(TStateClientTest, LifecycleSyncSkipsDeadClients)
{
    auto managerContext = MakeManagerContext();
    auto partitionStates = DynamicPointerCast<NTables::TInMemoryPartitionStates>(managerContext->PartitionStates);

    auto manager = MakeManager(managerContext);
    auto partitionCtx = manager->CreateContext()->AsPartition();

    {
        // Create client in inner scope, let it die.
        auto client = WaitFor(partitionCtx->CreateMutableStateClient<i64>("counter"))
            .ValueOrThrow();
        *client = 99;
        // Client goes out of scope here — weak ptr in manager becomes null.
    }

    // Sync should not crash even though the client is dead.
    EXPECT_NO_THROW(manager->Sync(/*transaction*/ nullptr));

    // Nothing was written since the client was dead at Sync time.
    EXPECT_EQ(partitionStates->GetWriteCount(), 0);
}

TEST_F(TStateClientTest, LifecycleSyncWritesLiveClients)
{
    auto managerContext = MakeManagerContext();
    auto partitionStates = DynamicPointerCast<NTables::TInMemoryPartitionStates>(managerContext->PartitionStates);

    auto manager = MakeManager(managerContext);
    auto partitionCtx = manager->CreateContext()->AsPartition();

    auto client = WaitFor(partitionCtx->CreateMutableStateClient<i64>("counter"))
        .ValueOrThrow();
    *client = 55;

    manager->Sync(/*transaction*/ nullptr);

    EXPECT_EQ(partitionStates->GetWriteCount(), 1);
}

////////////////////////////////////////////////////////////////////////////////
// TJobStateManager — preload callbacks

TEST_F(TStateClientTest, ManagerHasPreloadCallbacksAfterKeyClientCreated)
{
    auto managerContext = MakeManagerContext();
    auto manager = MakeManager(managerContext);
    EXPECT_FALSE(manager->HasPreloadCallbacks());

    auto keyClient = WaitFor(manager->CreateContext()->CreateMutableStateKeyClient<i64>("counter"))
        .ValueOrThrow();

    EXPECT_TRUE(manager->HasPreloadCallbacks());
}

TEST_F(TStateClientTest, ManagerPartitionClientHasNoPreloadCallbacks)
{
    auto managerContext = MakeManagerContext();
    auto manager = MakeManager(managerContext);
    auto client = WaitFor(manager->CreateContext()->AsPartition()->CreateMutableStateClient<i64>("counter"))
        .ValueOrThrow();

    EXPECT_FALSE(manager->HasPreloadCallbacks());
}

TEST_F(TStateClientTest, ManagerKeyClientHasPreloadCallbacks)
{
    auto managerContext = MakeManagerContext();
    auto manager = MakeManager(managerContext);
    auto key = MakeKey<ui64>(1);
    auto client = WaitFor(manager->CreateContext()->AsKey(key)->CreateMutableStateClient<i64>("counter"))
        .ValueOrThrow();

    // AsKey() creates a TKeyMutableStateProvider, not a TJobMutableStateKeyProvider,
    // so it does NOT add preload callbacks.
    EXPECT_FALSE(manager->HasPreloadCallbacks());
}

////////////////////////////////////////////////////////////////////////////////
// Isolation — prefix, different names, partition vs key

TEST_F(TStateClientTest, IsolationBasic)
{
    // Verify that:
    // - different state names are isolated
    // - partition and key states with the same name are isolated
    // - prefixed contexts are isolated
    auto managerContext = MakeManagerContext();
    auto key = MakeKey<ui64>(1);

    // Write to various isolated contexts.
    {
        auto manager = MakeManager(managerContext);
        auto ctx = manager->CreateContext();

        // Different names on partition.
        auto state_a = WaitFor(ctx->AsPartition()->CreateMutableStateClient<i64>("state_a")).ValueOrThrow();
        auto state_b = WaitFor(ctx->AsPartition()->CreateMutableStateClient<i64>("state_b")).ValueOrThrow();

        // Same name but partition vs key.
        auto partition_counter = WaitFor(ctx->AsPartition()->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        auto key_counter = WaitFor(ctx->AsKey(key)->CreateMutableStateClient<i64>("counter")).ValueOrThrow();

        // Prefixed contexts.
        auto prefix_a = WaitFor(ctx->WithPrefix("groupA")->AsPartition()->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        auto prefix_b = WaitFor(ctx->WithPrefix("groupB")->AsPartition()->CreateMutableStateClient<i64>("counter")).ValueOrThrow();

        *state_a = 1;
        *state_b = 2;
        *partition_counter = 100;
        *key_counter = 200;
        *prefix_a = 11;
        *prefix_b = 22;
        manager->Sync(/*transaction*/ nullptr);
    }

    // Read back and verify isolation.
    {
        auto manager = MakeManager(managerContext);
        auto ctx = manager->CreateContext();

        auto state_a = WaitFor(ctx->AsPartition()->CreateMutableStateClient<i64>("state_a")).ValueOrThrow();
        auto state_b = WaitFor(ctx->AsPartition()->CreateMutableStateClient<i64>("state_b")).ValueOrThrow();
        auto partition_counter = WaitFor(ctx->AsPartition()->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        auto key_counter = WaitFor(ctx->AsKey(key)->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        auto prefix_a = WaitFor(ctx->WithPrefix("groupA")->AsPartition()->CreateMutableStateClient<i64>("counter")).ValueOrThrow();
        auto prefix_b = WaitFor(ctx->WithPrefix("groupB")->AsPartition()->CreateMutableStateClient<i64>("counter")).ValueOrThrow();

        EXPECT_EQ(*state_a, 1);
        EXPECT_EQ(*state_b, 2);
        EXPECT_EQ(*partition_counter, 100);
        EXPECT_EQ(*key_counter, 200);
        EXPECT_EQ(*prefix_a, 11);
        EXPECT_EQ(*prefix_b, 22);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TStateClientTest, AccessorDefaultIsUninitialized)
{
    TConstStateAccessor<i64> accessor;
    EXPECT_FALSE(accessor.IsInitialized());
    EXPECT_EQ(accessor.Get(), nullptr);
}

TEST_F(TStateClientTest, AccessorHolderBackedIsInitialized)
{
    auto holder = New<TStateHolder<i64>>(7);
    TConstStateAccessor<i64> accessor(holder);
    EXPECT_TRUE(accessor.IsInitialized());
    EXPECT_EQ(*accessor, 7);
}

TEST_F(TStateClientTest, AccessorDereferenceThrowsWhenUninitialized)
{
    TConstStateAccessor<i64> accessor;
    EXPECT_THROW(*accessor, TErrorException);
    EXPECT_THROW(accessor.IsEmpty(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
