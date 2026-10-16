#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

using ::testing::_;

////////////////////////////////////////////////////////////////////////////////

class TFixedClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TFixedClientsCache(NApi::IClientPtr client)
        : Client_(std::move(client))
    { }

    NApi::IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return Client_;
    }

private:
    const NApi::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

//! Records the modifications the manager stages.
class TFakeRetryableTransaction
    : public IRetryableTransaction
{
public:
    struct TStagedModification
    {
        NYPath::TYPath Path;
        TNameTablePtr NameTable;
        TSharedRange<NApi::TRowModification> Modifications;
    };

    std::vector<TStagedModification> Staged;

    void ModifyRows(
        const NYPath::TYPath& path,
        TNameTablePtr nameTable,
        TSharedRange<NApi::TRowModification> modifications,
        const NApi::TModifyRowsOptions& /*options*/) override
    {
        Staged.push_back({path, std::move(nameTable), std::move(modifications)});
    }

    void WriteRows(
        const NYPath::TYPath& /*path*/,
        TNameTablePtr /*nameTable*/,
        TSharedRange<TUnversionedRow> /*rows*/,
        const NApi::TModifyRowsOptions& /*options*/,
        ELockType /*lockType*/) override
    {
        YT_ABORT();
    }

    void WriteRows(
        const NYPath::TYPath& /*path*/,
        TNameTablePtr /*nameTable*/,
        TSharedRange<TVersionedRow> /*rows*/,
        const NApi::TModifyRowsOptions& /*options*/) override
    {
        YT_ABORT();
    }

    void DeleteRows(
        const NYPath::TYPath& /*path*/,
        TNameTablePtr /*nameTable*/,
        TSharedRange<TLegacyKey> /*keys*/,
        const NApi::TModifyRowsOptions& /*options*/) override
    {
        YT_ABORT();
    }

    void LockRows(
        const NYPath::TYPath& /*path*/,
        TNameTablePtr /*nameTable*/,
        TSharedRange<TLegacyKey> /*keys*/,
        TLockMask /*lockMask*/) override
    {
        YT_ABORT();
    }

    void LockRows(
        const NYPath::TYPath& /*path*/,
        TNameTablePtr /*nameTable*/,
        TSharedRange<TLegacyKey> /*keys*/,
        ELockType /*lockType*/) override
    {
        YT_ABORT();
    }

    void LockRows(
        const NYPath::TYPath& /*path*/,
        TNameTablePtr /*nameTable*/,
        TSharedRange<TLegacyKey> /*keys*/,
        const std::vector<std::string>& /*locks*/,
        ELockType /*lockType*/) override
    {
        YT_ABORT();
    }

    void Apply(TCallback<void(const NApi::ITransactionPtr&)> /*transactionWriter*/) override
    {
        YT_ABORT();
    }

    void DoAttempt(const NApi::ITransactionPtr& /*transaction*/) override
    {
        YT_ABORT();
    }

    bool IsEmpty() override
    {
        return Staged.empty();
    }

    void SubscribeOnAttemptResult(TOnAttemptResultCallback /*callback*/) override
    { }

    void OnAttemptResult(const TCommitAttemptResult& /*result*/) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleExternalStateManagerTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr Queue_ = New<TActionQueue>("SimpleExternalStateManagerTest");
    const TIntrusivePtr<NApi::TMockClient> Client_ = New<NApi::TMockClient>();
    const TTableSchemaPtr KeySchema_ = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("hash", EValueType::Uint64, ESortOrder::Ascending),
        TColumnSchema("key", EValueType::String, ESortOrder::Ascending),
    });
    const TTableSchemaPtr TableSchema_ = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("hash", EValueType::Uint64, ESortOrder::Ascending),
        TColumnSchema("key", EValueType::String, ESortOrder::Ascending),
        TColumnSchema("payload", EValueType::String),
    });

    //! Key words of every LookupRows call, in call order.
    std::vector<std::vector<std::string>> LookedUp_;

    //! The state cache must be built on a serialized invoker, hence construction on the queue.
    TSimpleExternalStateManagerPtr MakeManager()
    {
        return WaitFor(BIND(&TSimpleExternalStateManagerTest::DoMakeManager, this)
                .AsyncVia(Queue_->GetInvoker())
                .Run())
            .ValueOrThrow();
    }

    TSimpleExternalStateManagerPtr DoMakeManager()
    {
        auto context = New<TExternalStateManagerContext>();
        context->KeySchema = KeySchema_;
        context->ClientsCache = New<TFixedClientsCache>(Client_);
        context->SerializedInvoker = Queue_->GetInvoker();
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->PipelinePath = NYPath::TRichYPath("//pipeline");
        context->PipelinePath.SetCluster("test");
        context->Logger = NLogging::TLogger("Test");
        context->StateCache = New<TStateCache>(New<TDynamicStateCacheSpec>(), NProfiling::TProfiler{})
            ->WithJob(TJobId(TGuid::Create()), NProfiling::TProfiler{})
            ->WithName("manager");

        auto spec = New<TExternalStateManagerSpec>();
        spec->ExternalStateManagerClassName = "NYT::NFlow::TSimpleExternalStateManager";
        spec->Parameters = BuildYsonNodeFluently()
            .BeginMap()
            .Item("path")
            .Value("<cluster=test>//table")
            .EndMap()
            ->AsMap();
        context->ExternalStateManagerSpec = std::move(spec);

        auto dynamicContext = New<TDynamicExternalStateManagerContext>();
        dynamicContext->DynamicExternalStateManagerSpec = New<TDynamicExternalStateManagerSpec>();

        return New<TSimpleExternalStateManager>(std::move(context), std::move(dynamicContext));
    }

    //! Serves every lookup with one stored row per key, payload "p:<key>", recording the
    //! requested keys.
    void ExpectLookups(int times)
    {
        EXPECT_CALL(*Client_, LookupRows(NYPath::TYPath("//table"), _, _, _))
            .Times(times)
            .WillRepeatedly([this] (
                const NYPath::TYPath& /*path*/,
                TNameTablePtr /*nameTable*/,
                const TSharedRange<TLegacyKey>& keys,
                const NApi::TLookupRowsOptions& /*options*/) {
                std::vector<std::string> words;
                auto owningRows = std::make_shared<std::vector<TUnversionedOwningRow>>();
                std::vector<TUnversionedRow> rows;
                for (const auto& lookupKey : keys) {
                    auto hash = lookupKey[0].Data.Uint64;
                    auto word = std::string(lookupKey[1].AsStringBuf());
                    words.push_back(word);
                    TUnversionedOwningRowBuilder builder;
                    builder.AddValue(MakeUnversionedUint64Value(hash, 0));
                    builder.AddValue(MakeUnversionedStringValue(word, 1));
                    builder.AddValue(MakeUnversionedStringValue("p:" + word, 2));
                    owningRows->push_back(builder.FinishRow());
                    rows.push_back(owningRows->back());
                }
                LookedUp_.push_back(std::move(words));
                NApi::TUnversionedLookupRowsResult result;
                result.Rowset = NApi::CreateRowset(TableSchema_, MakeSharedRange(std::move(rows), std::move(owningRows)));
                return MakeFuture(std::move(result));
            });
    }

    //! No lookup may reach the client.
    void ExpectNoLookups()
    {
        EXPECT_CALL(*Client_, LookupRows(_, _, _, _)).Times(0);
    }

    //! Syncs and asserts the single staged modification is a delete of (|hash|, |word|).
    static void ExpectSingleDelete(
        const TSimpleExternalStateManagerPtr& manager,
        ui64 hash,
        const std::string& word)
    {
        auto transaction = New<TFakeRetryableTransaction>();
        manager->Sync(transaction);

        ASSERT_EQ(std::ssize(transaction->Staged), 1);
        const auto& staged = transaction->Staged[0];
        EXPECT_EQ(staged.Path, "//table");
        ASSERT_EQ(std::ssize(staged.Modifications), 1);
        const auto* deletion = std::get_if<NApi::NRowModifications::TDeleteRow>(&staged.Modifications[0]);
        ASSERT_TRUE(deletion);
        ASSERT_EQ(deletion->Key.GetCount(), 2u);
        EXPECT_EQ(staged.NameTable->GetName(deletion->Key[0].Id), "hash");
        EXPECT_EQ(deletion->Key[0].Data.Uint64, hash);
        EXPECT_EQ(staged.NameTable->GetName(deletion->Key[1].Id), "key");
        EXPECT_EQ(deletion->Key[1].AsStringBuf(), word);
    }

    //! Preload and the lookup continuation need a fiber and a serialized invoker.
    void Preload(const TSimpleExternalStateManagerPtr& manager, THashSet<TKey> keys)
    {
        WaitFor(BIND([manager, keys = std::move(keys)] {
            WaitFor(manager->PreloadKeyStates(keys)).ThrowOnError();
        })
                .AsyncVia(Queue_->GetInvoker())
                .Run())
            .ThrowOnError();
    }

    static TIntrusivePtr<TStateHolder<TSimpleExternalState>> GetTypedState(
        const TSimpleExternalStateManagerPtr& manager,
        const TKey& key)
    {
        auto state = DynamicPointerCast<TStateHolder<TSimpleExternalState>>(manager->GetState(key));
        YT_VERIFY(state);
        return state;
    }

    static std::string GetPayload(const TSimpleExternalStateManagerPtr& manager, const TKey& key)
    {
        return GetTypedState(manager, key)->Get().GetColumnValue<std::string>("payload");
    }

    static void SetPayload(const TSimpleExternalStateManagerPtr& manager, const TKey& key, const std::string& payload)
    {
        auto& state = GetTypedState(manager, key)->Get();
        TPayloadBuilder builder(state.Schema);
        builder.Set(payload, "payload");
        state.Payload = builder.Finish();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSimpleExternalStateManagerTest, GetStateThrowsWithoutPreload)
{
    auto manager = MakeManager();

    EXPECT_THROW_WITH_SUBSTRING(
        manager->GetState(MakeKey(1ul, "absent")),
        "has no preloaded state for key");
}

TEST_F(TSimpleExternalStateManagerTest, GetStateThrowsForKeyOutsidePreload)
{
    auto manager = MakeManager();
    ExpectLookups(1);

    Preload(manager, {MakeKey(1ul, "k1")});

    EXPECT_EQ(GetPayload(manager, MakeKey(1ul, "k1")), "p:k1");
    EXPECT_THROW_WITH_SUBSTRING(
        manager->GetState(MakeKey(2ul, "k2")),
        "has no preloaded state for key");
}

TEST_F(TSimpleExternalStateManagerTest, SecondPreloadLoadsOnlyNewKeys)
{
    auto manager = MakeManager();
    ExpectLookups(2);

    Preload(manager, {MakeKey(1ul, "k1")});
    Preload(manager, {MakeKey(1ul, "k1"), MakeKey(2ul, "k2")});

    ASSERT_EQ(std::ssize(LookedUp_), 2);
    EXPECT_EQ(LookedUp_[0], std::vector<std::string>{"k1"});
    EXPECT_EQ(LookedUp_[1], std::vector<std::string>{"k2"});
    EXPECT_EQ(GetPayload(manager, MakeKey(1ul, "k1")), "p:k1");
    EXPECT_EQ(GetPayload(manager, MakeKey(2ul, "k2")), "p:k2");
}

TEST_F(TSimpleExternalStateManagerTest, SecondPreloadKeepsMutatedState)
{
    auto manager = MakeManager();
    ExpectLookups(1);

    auto key = MakeKey(1ul, "k1");
    Preload(manager, {key});
    SetPayload(manager, key, "mutated");

    // Already loaded in this epoch: no lookup, no rollback.
    Preload(manager, {key});

    EXPECT_EQ(GetPayload(manager, key), "mutated");
}

TEST_F(TSimpleExternalStateManagerTest, EraseWithoutPreloadStagesDeleteRow)
{
    auto manager = MakeManager();
    ExpectNoLookups();

    // No schema is known in an erase-only epoch; deletes carry key columns only.
    manager->EraseKeyState(MakeKey(7ul, "expired"));

    ExpectSingleDelete(manager, 7, "expired");
}

TEST_F(TSimpleExternalStateManagerTest, EraseLoadedStateMakesKeyUnreadable)
{
    auto manager = MakeManager();
    ExpectLookups(1);

    auto key = MakeKey(1ul, "k1");
    Preload(manager, {key});
    SetPayload(manager, key, "mutated");
    manager->EraseKeyState(key);

    // Terminal: unreadable, and a repeated preload neither looks the key up nor brings it back.
    EXPECT_THROW_WITH_SUBSTRING(manager->GetState(key), "was erased in this epoch");
    Preload(manager, {key});
    EXPECT_THROW_WITH_SUBSTRING(manager->GetState(key), "was erased in this epoch");
    ExpectSingleDelete(manager, 1, "k1");
}

TEST_F(TSimpleExternalStateManagerTest, PreloadAfterEraseSkipsLookup)
{
    auto manager = MakeManager();
    ExpectNoLookups();

    auto key = MakeKey(1ul, "k1");
    manager->EraseKeyState(key);
    Preload(manager, {key});

    // An erased key is gone for the rest of the epoch.
    EXPECT_THROW_WITH_SUBSTRING(manager->GetState(key), "was erased in this epoch");
}

TEST_F(TSimpleExternalStateManagerTest, EraseEvictsCachedState)
{
    auto manager = MakeManager();
    ExpectLookups(2);

    auto key = MakeKey(1ul, "k1");
    Preload(manager, {key});
    manager->Sync(New<TFakeRetryableTransaction>()); // Caches k1.

    manager->EraseKeyState(key);
    ExpectSingleDelete(manager, 1, "k1");

    // The cached row must not resurrect the deleted state: the next epoch looks it up.
    Preload(manager, {key});
    ASSERT_EQ(std::ssize(LookedUp_), 2);
    EXPECT_EQ(LookedUp_[1], std::vector<std::string>{"k1"});
}

TEST_F(TSimpleExternalStateManagerTest, SyncWithoutPreloadIsNoOp)
{
    auto manager = MakeManager();

    // A null transaction proves no write path is entered.
    EXPECT_NO_THROW(manager->Sync(/*transaction*/ nullptr));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
