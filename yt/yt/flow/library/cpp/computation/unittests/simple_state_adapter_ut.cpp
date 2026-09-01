#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/companion_state_adapter.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/static_table_key_visitor_joiner.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

using ::testing::_;
using ::testing::Invoke;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TSharedMockClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TSharedMockClientsCache(NApi::IClientPtr client)
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

//! Minimal init context over fixed manager/joiner registries; only the members
//! used by the companion-adapter helpers are implemented.
class TFakeInitContext
    : public IJobInitContext
{
public:
    TFakeInitContext(
        THashMap<std::string, IExternalStateManagerPtr> managers,
        THashMap<std::string, IExternalStateJoinerPtr> joiners,
        std::string prefix = {})
        : Managers_(std::move(managers))
        , Joiners_(std::move(joiners))
        , Prefix_(std::move(prefix))
    { }

    TFuture<IMutableStateKeyProviderPtr> CreateMutableStateKeyProvider(std::function<IStateHolderPtr()> /*ctor*/) const final
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<IJoinedStateKeyProviderPtr> CreateJoinedStateKeyProvider(std::function<IStateHolderPtr()> /*ctor*/) const final
    {
        YT_UNIMPLEMENTED();
    }

    IInitContextPtr AsPartition() const final
    {
        YT_UNIMPLEMENTED();
    }

    IInitContextPtr AsKey(TKey /*key*/) const final
    {
        YT_UNIMPLEMENTED();
    }

    IJobInitContextPtr WithPrefix(TStringBuf prefix) const final
    {
        return New<TFakeInitContext>(Managers_, Joiners_, std::string(prefix));
    }

    const std::string& GetPrefix() const final
    {
        return Prefix_;
    }

protected:
    IExternalStateManagerPtr GetExternalStateManagerOrThrow(const std::string& name) const final
    {
        if (auto* manager = Managers_.FindPtr(name)) {
            return *manager;
        }
        THROW_ERROR_EXCEPTION("Unknown external state manager %Qv", name);
    }

    IExternalStateJoinerPtr GetExternalStateJoinerOrThrow(const std::string& name) const final
    {
        if (auto* joiner = Joiners_.FindPtr(name)) {
            return *joiner;
        }
        THROW_ERROR_EXCEPTION("Unknown external state joiner %Qv", name);
    }

private:
    const THashMap<std::string, IExternalStateManagerPtr> Managers_;
    const THashMap<std::string, IExternalStateJoinerPtr> Joiners_;
    const std::string Prefix_;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleStateAdapterTest
    : public ::testing::Test
{
protected:
    const TTableSchemaPtr KeySchema_ = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("hash", EValueType::Uint64, ESortOrder::Ascending),
        TColumnSchema("key", EValueType::String, ESortOrder::Ascending),
    });
    const TTableSchemaPtr StateSchema_ = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("a", EValueType::Int64),
        TColumnSchema("b", EValueType::String),
    });

    const TIntrusivePtr<NApi::TMockClient> MockClient_ = New<NApi::TMockClient>();
    const TActionQueuePtr Queue_ = New<TActionQueue>("SimpleStateAdapterTest");

    //! Rows the mock lookup serves, hash -> (a, b).
    THashMap<ui64, std::pair<i64, TString>> SeededRows_;

    void SetUp() override
    {
        EXPECT_CALL(*MockClient_, LookupRows(_, _, _, _))
            .WillRepeatedly(Invoke([this] (
                const NYPath::TYPath& /*path*/,
                TNameTablePtr /*nameTable*/,
                const TSharedRange<TLegacyKey>& keys,
                const NApi::TLookupRowsOptions& /*options*/) {
                return MakeFuture(MakeLookupResult(keys));
            }));
    }

    TPayload MakeStatePayload(i64 integerValue, TStringBuf stringValue)
    {
        TPayloadBuilder builder(StateSchema_);
        builder.Set<i64>(integerValue, "a");
        builder.Set<TStringBuf>(stringValue, "b");
        return builder.Finish();
    }

    TPayload MakeEmptyStatePayload()
    {
        return TPayloadBuilder(StateSchema_).Finish();
    }

    static TProtobufString SerializePayload(const TPayload& payload)
    {
        return ToProto<TProtobufString>(payload);
    }

    static TStringBuf AsStringBuf(const TSharedRef& ref)
    {
        return TStringBuf(ref.Begin(), ref.Size());
    }

    NApi::TUnversionedLookupRowsResult MakeLookupResult(const TSharedRange<TLegacyKey>& keys)
    {
        auto resultSchema = New<TTableSchema>(ConcatVectors(KeySchema_->Columns(), StateSchema_->Columns()));

        auto owningRows = std::make_shared<std::vector<TUnversionedOwningRow>>();
        std::vector<TUnversionedRow> rows;
        for (const auto& key : keys) {
            TUnversionedOwningRowBuilder builder;
            auto hash = key[0].Data.Uint64;
            if (auto* seeded = SeededRows_.FindPtr(hash)) {
                auto keyValue = key[1];
                keyValue.Id = 1;
                builder.AddValue(MakeUnversionedUint64Value(hash, 0));
                builder.AddValue(keyValue);
                builder.AddValue(MakeUnversionedInt64Value(seeded->first, 2));
                builder.AddValue(MakeUnversionedStringValue(seeded->second, 3));
            }
            owningRows->push_back(builder.FinishRow());
            rows.push_back(owningRows->back());
        }

        return NApi::TUnversionedLookupRowsResult{
            .Rowset = NApi::CreateRowset(std::move(resultSchema), MakeSharedRange(std::move(rows), owningRows)),
        };
    }

    template <class TContext>
    TIntrusivePtr<TContext> MakeContext()
    {
        auto context = New<TContext>();
        context->KeySchema = KeySchema_;
        context->StateCache = New<TStateCache>(New<TDynamicStateCacheSpec>(), NProfiling::TProfiler{})
            ->WithJob(TJobId(TGuid::Create()), NProfiling::TProfiler{})
            ->WithName("test");
        context->ClientsCache = New<TSharedMockClientsCache>(MockClient_);
        context->SerializedInvoker = Queue_->GetInvoker();
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->PipelinePath = NYPath::TRichYPath("//pipeline");
        context->PipelinePath.SetCluster("test");
        context->Logger = NLogging::TLogger("Test");
        return context;
    }

    TExternalStateManagerContextPtr MakeManagerContext()
    {
        auto context = MakeContext<TExternalStateManagerContext>();

        auto spec = New<TExternalStateManagerSpec>();
        spec->ExternalStateManagerClassName = "NYT::NFlow::TSimpleExternalStateManager";
        spec->Parameters = BuildYsonNodeFluently()
            .BeginMap()
            .Item("path")
            .Value("//state")
            .EndMap()
            ->AsMap();
        context->ExternalStateManagerSpec = std::move(spec);
        return context;
    }

    TSimpleExternalStateManagerPtr MakeManager()
    {
        auto dynamicContext = New<TDynamicExternalStateManagerContext>();
        dynamicContext->DynamicExternalStateManagerSpec = New<TDynamicExternalStateManagerSpec>();
        return New<TSimpleExternalStateManager>(MakeManagerContext(), std::move(dynamicContext));
    }

    TExternalStateJoinerContextPtr MakeJoinerContext(TStringBuf className)
    {
        auto context = MakeContext<TExternalStateJoinerContext>();

        auto spec = New<TExternalStateJoinerSpec>();
        spec->ExternalStateJoinerClassName = std::string(className);
        spec->JoinOn = New<TStateJoinSpec>();
        spec->Parameters = BuildYsonNodeFluently()
            .BeginMap()
            .Item("path")
            .Value("//state")
            .EndMap()
            ->AsMap();
        context->ExternalStateJoinerSpec = std::move(spec);
        return context;
    }

    static TDynamicExternalStateJoinerContextPtr MakeDynamicJoinerContext()
    {
        auto dynamicContext = New<TDynamicExternalStateJoinerContext>();
        dynamicContext->DynamicExternalStateJoinerSpec = New<TDynamicExternalStateJoinerSpec>();
        return dynamicContext;
    }

    TSimpleExternalStateJoinerPtr MakeJoiner()
    {
        return New<TSimpleExternalStateJoiner>(
            MakeJoinerContext("NYT::NFlow::TSimpleExternalStateJoiner"),
            MakeDynamicJoinerContext());
    }

    static const TSimpleExternalState& AsSimpleState(const IStateHolderPtr& holder)
    {
        auto typed = DynamicPointerCast<TStateHolder<TSimpleExternalState>>(holder);
        YT_VERIFY(typed);
        return typed->Get();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSimpleStateAdapterTest, ManagerDescriptor)
{
    auto manager = MakeManager();
    auto adapter = manager->CreateCompanionAdapter("profile");
    ASSERT_TRUE(adapter);

    auto descriptor = adapter->Describe();
    EXPECT_EQ(descriptor.StateName, "profile");
    EXPECT_EQ(descriptor.Format, EStateFormat::SimpleRow);
    // The state schema is discovered at preload.
    EXPECT_FALSE(descriptor.Schema);

    SeededRows_[1] = {10, "x"};
    WaitFor(manager->PreloadKeyStates({MakeKey(ui64(1), "a")})).ThrowOnError();

    descriptor = adapter->Describe();
    ASSERT_TRUE(descriptor.Schema);
    EXPECT_EQ(*descriptor.Schema, *StateSchema_->ToCanonical());
}

TEST_F(TSimpleStateAdapterTest, ManagerEncodeReproducesWireBytes)
{
    auto manager = MakeManager();
    auto adapter = manager->CreateCompanionAdapter("profile");

    SeededRows_[1] = {10, "x"};
    auto presentKey = MakeKey(ui64(1), "a");
    auto absentKey = MakeKey(ui64(2), "b");
    WaitFor(manager->PreloadKeyStates({presentKey, absentKey})).ThrowOnError();

    // A present state serializes exactly as the pre-adapter wire path did.
    auto encoded = adapter->EncodeState(presentKey);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(AsStringBuf(encoded), TStringBuf(SerializePayload(MakeStatePayload(10, "x"))));

    // A missing row loads as an empty state and is still sent (as before).
    auto encodedAbsent = adapter->EncodeState(absentKey);
    ASSERT_TRUE(encodedAbsent);
    EXPECT_EQ(AsStringBuf(encodedAbsent), TStringBuf(SerializePayload(MakeEmptyStatePayload())));
}

TEST_F(TSimpleStateAdapterTest, ManagerApplyModifiedPayload)
{
    auto manager = MakeManager();
    auto adapter = manager->CreateCompanionAdapter("profile");

    SeededRows_[1] = {10, "x"};
    auto key = MakeKey(ui64(1), "a");
    WaitFor(manager->PreloadKeyStates({key})).ThrowOnError();

    auto modified = SerializePayload(MakeStatePayload(20, "y"));
    adapter->ApplyState(key, TSharedRef::FromString(TString(modified)));

    const auto& state = AsSimpleState(manager->GetState(key));
    EXPECT_EQ(state.GetColumnValue<i64>("a"), 20);
    EXPECT_EQ(state.GetColumnValue<TString>("b"), "y");
    EXPECT_EQ(AsStringBuf(adapter->EncodeState(key)), TStringBuf(modified));
}

TEST_F(TSimpleStateAdapterTest, ManagerResetState)
{
    auto manager = MakeManager();
    auto adapter = manager->CreateCompanionAdapter("profile");

    SeededRows_[1] = {10, "x"};
    auto key = MakeKey(ui64(1), "a");
    WaitFor(manager->PreloadKeyStates({key})).ThrowOnError();

    adapter->ResetState(key);

    EXPECT_TRUE(manager->GetState(key)->IsEmpty());
    EXPECT_EQ(AsStringBuf(adapter->EncodeState(key)), TStringBuf(SerializePayload(MakeEmptyStatePayload())));
}

TEST_F(TSimpleStateAdapterTest, JoinerAdapterIsReadOnly)
{
    auto joiner = MakeJoiner();
    auto adapter = joiner->CreateCompanionAdapter("joined");
    ASSERT_TRUE(adapter);

    auto descriptor = adapter->Describe();
    EXPECT_EQ(descriptor.StateName, "joined");
    EXPECT_EQ(descriptor.Format, EStateFormat::SimpleRow);

    auto key = MakeKey(ui64(1), "a");
    EXPECT_THROW_WITH_SUBSTRING(
        adapter->ApplyState(key, TSharedRef::FromString(TString("payload"))),
        "read-only");
    EXPECT_THROW_WITH_SUBSTRING(adapter->ResetState(key), "read-only");
}

TEST_F(TSimpleStateAdapterTest, JoinerEncodeReproducesWireBytes)
{
    auto joiner = MakeJoiner();
    auto adapter = joiner->CreateCompanionAdapter("joined");

    SeededRows_[1] = {10, "x"};
    auto key = MakeKey(ui64(1), "a");
    WaitFor(joiner->PreloadKeyStates({key})).ThrowOnError();

    auto encoded = adapter->EncodeState(key);
    ASSERT_TRUE(encoded);
    EXPECT_EQ(AsStringBuf(encoded), TStringBuf(SerializePayload(MakeStatePayload(10, "x"))));
}

TEST_F(TSimpleStateAdapterTest, InitContextResolvesAdapter)
{
    auto manager = MakeManager();
    auto initContext = New<TFakeInitContext>(
        THashMap<std::string, IExternalStateManagerPtr>{{"profile", manager}},
        THashMap<std::string, IExternalStateJoinerPtr>{});

    auto adapter = initContext->CreateCompanionStateAdapter("profile");
    ASSERT_TRUE(adapter);
    EXPECT_EQ(adapter->Describe().StateName, "profile");

    EXPECT_THROW_WITH_SUBSTRING(
        initContext->CreateCompanionStateAdapter("unknown"),
        "Unknown external state manager");
}

TEST_F(TSimpleStateAdapterTest, MissingAdapterIsSpecValidationError)
{
    // TStaticTableKeyVisitorJoiner does not override CreateCompanionAdapter, so the
    // default null adapter must surface as an init error naming the class.
    auto joiner = New<TStaticTableKeyVisitorJoiner>(
        MakeJoinerContext("NYT::NFlow::TStaticTableKeyVisitorJoiner"),
        MakeDynamicJoinerContext());
    auto initContext = New<TFakeInitContext>(
        THashMap<std::string, IExternalStateManagerPtr>{},
        THashMap<std::string, IExternalStateJoinerPtr>{{"joined", joiner}});

    EXPECT_THROW_WITH_SUBSTRING(
        initContext->CreateJoinedCompanionStateAdapter("joined"),
        "does not support companion computations");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
