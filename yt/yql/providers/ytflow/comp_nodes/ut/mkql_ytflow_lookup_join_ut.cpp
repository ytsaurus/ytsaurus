#include <yt/yql/providers/ytflow/comp_nodes/mkql_ytflow_lookup_join.h>
#include <yt/yql/providers/yt/mkql_ytflow/yql_yt_ytflow_lookup_provider.h>
#include <yt/yql/providers/ytflow/integration/mkql_interface/yql_ytflow_lookup_provider.h>

#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>
#include <yql/essentials/public/udf/udf_type_builder.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/datetime/base.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <utility>

namespace NYql {
namespace {

using namespace NKikimr::NMiniKQL;

class TProviderCreationContext {
public:
    explicit TProviderCreationContext(TStringBuf name)
        : Alloc_(__LOCATION__)
        , Env_(Alloc_)
        , MemoryUsageInfo_(name)
        , HolderFactory_(Alloc_.Ref(), MemoryUsageInfo_)
        , ValueBuilder_(HolderFactory_)
        , RuntimeSettings_(MakeRuntimeSettings())
        , FunctionTypeInfoBuilder_(
              UnknownLangVersion,
              *RuntimeSettings_,
              Env_,
              new TTypeInfoHelper(),
              name,
              /*countersProvider*/ nullptr,
              NUdf::TSourcePosition())
    {
    }

    IYtflowLookupProviderFactory::TCreationContext GetContext()
    {
        return {
            .ValueBuilder = ValueBuilder_,
            .FunctionTypeInfoBuilder = FunctionTypeInfoBuilder_,
        };
    }

private:
    TScopedAlloc Alloc_;
    TTypeEnvironment Env_;
    TMemoryUsageInfo MemoryUsageInfo_;
    THolderFactory HolderFactory_;
    TDefaultValueBuilder ValueBuilder_;
    TRuntimeSettings::TConstPtr RuntimeSettings_;
    TFunctionTypeInfoBuilder FunctionTypeInfoBuilder_;
};

class TLookupProviderFactoryHolder {
public:
    TLookupProviderFactoryHolder()
        : PatternAlloc_(__LOCATION__)
        , PatternEnv_(PatternAlloc_)
    {
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TProgramBuilder programBuilder(PatternEnv_, *functionRegistry);

        auto* keyType = programBuilder.NewDataType(NUdf::EDataSlot::String);
        const TVector<std::pair<std::string_view, TType*>> streamMembers = {
            {"key", keyType},
        };
        auto* streamRowType = AS_TYPE(
            TStructType,
            programBuilder.NewStructType(streamMembers));
        const TVector<std::pair<std::string_view, TType*>> lookupSourceMembers = {
            {"key", keyType},
            {"value", programBuilder.NewDataType(NUdf::EDataSlot::Int64)},
        };
        auto* lookupSourceRowType = AS_TYPE(
            TStructType,
            programBuilder.NewStructType(lookupSourceMembers));
        auto lookupSourceArgs = programBuilder.NewTuple({
            programBuilder.NewDataLiteral<NUdf::EDataSlot::String>("localhost"),
            programBuilder.NewDataLiteral<NUdf::EDataSlot::String>("//test/table"),
            programBuilder.NewDataLiteral<NUdf::EDataSlot::String>("test_token"),
        });

        // The registry and secure-params provider model the temporary
        // factory-creation context and are destroyed before a provider is
        // created.
        auto registry = CreateYtflowLookupProviderRegistry();
        RegisterYtYtflowLookupProvider(*registry);

        auto secureParamsProvider = MakeSimpleSecureParamsProvider({
            {"test_token", "token_value"},
        });
        const auto factoryCreationContext =
            IYtflowLookupProviderRegistry::TFactoryCreationContext{
            .LookupSourceArgs = lookupSourceArgs,
            .LookupSourceRowSelectionMode = ERowSelectionMode::Any,
            .StreamKeys = {"key"},
            .StreamRowType = streamRowType,
            .LookupSourceKeys = {"key"},
            .LookupSourceRowType = lookupSourceRowType,
            .TypeEnvironment = PatternEnv_,
            .SecureParamsProvider = secureParamsProvider.get(),
        };
        Factory_ = registry->CreateFactory("yt", factoryCreationContext);
    }

    THolder<IYtflowLookupProvider> Create(
        TProviderCreationContext& context) const {
        return Factory_->Create(context.GetContext());
    }

private:
    // Models the pattern holder: it owns the types referenced by Factory_
    // and therefore must outlive the factory.
    TScopedAlloc PatternAlloc_;
    TTypeEnvironment PatternEnv_;
    THolder<IYtflowLookupProviderFactory> Factory_;
};

TEST(TLookupProviderFactoryTest, OutlivesFactoryCreationContext)
{
    TLookupProviderFactoryHolder factory;
    TProviderCreationContext context("lookup provider");
    auto provider = factory.Create(context);

    ASSERT_TRUE(provider);
    ASSERT_EQ("localhost.//test/table", provider->GetTableName());
}

TEST(TLookupProviderFactoryTest, CreatesIndependentProviders)
{
    TLookupProviderFactoryHolder factory;
    TProviderCreationContext firstContext("first lookup provider");
    TProviderCreationContext secondContext("second lookup provider");
    auto firstProvider = factory.Create(firstContext);
    auto secondProvider = factory.Create(secondContext);

    ASSERT_TRUE(firstProvider);
    ASSERT_TRUE(secondProvider);
    ASSERT_NE(firstProvider.Get(), secondProvider.Get());
    ASSERT_EQ(firstProvider->GetTableName(), secondProvider->GetTableName());
}

class TFakeLookupResult
    : public IYtflowLookupProvider::ILookupResult {
public:
    explicit TFakeLookupResult(ui64 key)
        : Key(key)
    {
    }

    const ui64 Key;
};

enum class ELookupMode {
    Ready,
    Controlled,
};

struct TProviderProbe {
    ui64 ProviderId = 0;
    const void* ProviderIdentity = nullptr;
    const void* CodecIdentity = nullptr;
    const void* BufferIdentity = nullptr;
    const NUdf::IValueBuilder* ValueBuilder = nullptr;
    ui32 LookupCount = 0;
    ui32 DecodeCount = 0;
    bool Destroyed = false;
};

class TAsyncCompletionProbe {
public:
    TAsyncCompletionProbe()
        : Promise_(NThreading::NewPromise<void>())
    {
    }

    void Complete()
    {
        Count_.fetch_add(1);
        Promise_.SetValue();
    }

    bool Wait(TDuration timeout) const {
        return Promise_.GetFuture().Wait(timeout);
    }

    ui32 GetCount() const {
        return Count_.load();
    }

private:
    NThreading::TPromise<void> Promise_;
    std::atomic<ui32> Count_ = 0;
};

class TFakeLookupState {
public:
    explicit TFakeLookupState(ELookupMode mode)
        : Mode_(mode)
    {
    }

    size_t AddProvider(
        const void* providerIdentity,
        const void* codecIdentity,
        const void* bufferIdentity,
        const NUdf::IValueBuilder* valueBuilder)
    {
        std::lock_guard guard(Mutex_);
        Providers_.push_back(TProviderState{
            .Probe = {
                .ProviderId = NextProviderId_++,
                .ProviderIdentity = providerIdentity,
                .CodecIdentity = codecIdentity,
                .BufferIdentity = bufferIdentity,
                .ValueBuilder = valueBuilder,
            },
            .CompletionProbe = std::make_shared<TAsyncCompletionProbe>(),
        });
        return Providers_.size() - 1;
    }

    TProviderProbe GetProbe(size_t index) const {
        std::lock_guard guard(Mutex_);
        return Providers_.at(index).Probe;
    }

    TVector<TProviderProbe> GetProbes() const {
        std::lock_guard guard(Mutex_);
        TVector<TProviderProbe> probes;
        probes.reserve(Providers_.size());
        for (const auto& provider : Providers_) {
            probes.push_back(provider.Probe);
        }
        return probes;
    }

    ELookupMode GetMode() const {
        return Mode_;
    }

    std::shared_ptr<TAsyncCompletionProbe> GetCompletionProbe(
        size_t index) const {
        std::lock_guard guard(Mutex_);
        return Providers_.at(index).CompletionProbe;
    }

    void OnLookup(size_t index)
    {
        std::lock_guard guard(Mutex_);
        ++Providers_.at(index).Probe.LookupCount;
    }

    void OnDecode(size_t index)
    {
        std::lock_guard guard(Mutex_);
        ++Providers_.at(index).Probe.DecodeCount;
    }

    void OnDestroyed(size_t index)
    {
        std::lock_guard guard(Mutex_);
        Providers_.at(index).Probe.Destroyed = true;
    }

    void SetPromise(
        size_t index,
        NThreading::TPromise<IYtflowLookupProvider::ILookupResultPtr> promise)
    {
        std::lock_guard guard(Mutex_);
        auto& provider = Providers_.at(index);
        Y_ABORT_UNLESS(!provider.PendingPromise.Initialized());
        provider.PendingPromise = std::move(promise);
    }

    NThreading::TPromise<IYtflowLookupProvider::ILookupResultPtr>
    ExtractPromise(size_t index)
    {
        std::lock_guard guard(Mutex_);
        auto& provider = Providers_.at(index);
        Y_ABORT_UNLESS(provider.PendingPromise.Initialized());
        auto promise = std::move(provider.PendingPromise);
        provider.PendingPromise = {};
        return promise;
    }

private:
    struct TProviderState {
        TProviderProbe Probe;
        NThreading::TPromise<IYtflowLookupProvider::ILookupResultPtr>
            PendingPromise;
        std::shared_ptr<TAsyncCompletionProbe> CompletionProbe;
    };

    const ELookupMode Mode_;
    mutable std::mutex Mutex_;
    TVector<TProviderState> Providers_;
    ui64 NextProviderId_ = 1;
};

class TFakeBufferProbe {
public:
    ui64 Encode(const NUdf::TUnboxedValue& key)
    {
        ++UseCount_;
        return key.GetElement(0).Get<ui64>();
    }

private:
    ui32 UseCount_ = 0;
};

class TFakeCodecProbe {
public:
    NUdf::TUnboxedValue Decode(
        NUdf::IValueBuilder& valueBuilder,
        ui64 key,
        ui64 value)
    {
        ++UseCount_;
        NUdf::TUnboxedValue* items = nullptr;
        auto row = valueBuilder.NewArray(2, items);
        items[0] = NUdf::TUnboxedValuePod(key);
        items[1] = NUdf::TUnboxedValuePod(value);
        return row;
    }

private:
    ui32 UseCount_ = 0;
};

class TFakeLookupProvider
    : public IYtflowLookupProvider {
public:
    TFakeLookupProvider(
        std::shared_ptr<TFakeLookupState> state,
        NUdf::IValueBuilder& valueBuilder)
        : State_(std::move(state))
        , ProbeIndex_(State_->AddProvider(
              this,
              &CodecProbe_,
              &BufferProbe_,
              &valueBuilder))
        , ValueBuilder_(valueBuilder)
    {
    }

    ~TFakeLookupProvider() override {
        State_->OnDestroyed(ProbeIndex_);
    }

    NThreading::TFuture<ILookupResultPtr> Lookup(
        const TVector<NUdf::TUnboxedValue>& keys) override {
        Y_ABORT_UNLESS(keys.size() == 1);
        const auto key = BufferProbe_.Encode(keys.front());
        State_->OnLookup(ProbeIndex_);

        if (State_->GetMode() == ELookupMode::Controlled) {
            auto promise = NThreading::NewPromise<ILookupResultPtr>();
            State_->SetPromise(ProbeIndex_, promise);
            auto completionProbe = State_->GetCompletionProbe(ProbeIndex_);
            return promise.GetFuture().Apply(
                [completionProbe](
                    const NThreading::TFuture<ILookupResultPtr>& future)
                {
                    completionProbe->Complete();
                    return future.GetValue();
                });
        }

        ILookupResultPtr result = std::make_shared<TFakeLookupResult>(key);
        return NThreading::MakeFuture(std::move(result));
    }

    TVector<TVector<NUdf::TUnboxedValue>> Decode(
        const ILookupResultPtr& result) override {
        State_->OnDecode(ProbeIndex_);
        const auto& lookupResult =
            static_cast<const TFakeLookupResult&>(*result);
        const auto probe = State_->GetProbe(ProbeIndex_);

        auto row = CodecProbe_.Decode(
            ValueBuilder_,
            lookupResult.Key,
            probe.ProviderId * 100 + lookupResult.Key);

        return {{std::move(row)}};
    }

    TString GetTableName() const override {
        return "fake.table";
    }

private:
    const std::shared_ptr<TFakeLookupState> State_;
    TFakeCodecProbe CodecProbe_;
    TFakeBufferProbe BufferProbe_;
    const size_t ProbeIndex_;
    NUdf::IValueBuilder& ValueBuilder_;
};

class TFakeLookupProviderFactory
    : public IYtflowLookupProviderFactory {
public:
    explicit TFakeLookupProviderFactory(
        std::shared_ptr<TFakeLookupState> state)
        : State_(std::move(state))
    {
    }

    THolder<IYtflowLookupProvider> Create(
        const TCreationContext& ctx) const override {
        return MakeHolder<TFakeLookupProvider>(State_, ctx.ValueBuilder);
    }

private:
    const std::shared_ptr<TFakeLookupState> State_;
};

TScopedAlloc& EnableRefLocking(TScopedAlloc& alloc)
{
    alloc.Ref().UseRefLocking = true;
    return alloc;
}

class TLookupJoinPattern {
public:
    TLookupJoinPattern(ELookupMode mode, TDuration timeout)
        : Alloc_(__LOCATION__)
        , Env_(EnableRefLocking(Alloc_))
        , FunctionRegistry_(CreateFunctionRegistry(CreateBuiltinRegistry()))
        , RuntimeSettings_(MakeRuntimeSettings())
        , State_(std::make_shared<TFakeLookupState>(mode))
        , LookupProviderRegistry_(CreateYtflowLookupProviderRegistry())
    {
        LookupProviderRegistry_->Register(
            "fake",
            [state = State_](
                const IYtflowLookupProviderRegistry::TFactoryCreationContext&)
            {
                return MakeHolder<TFakeLookupProviderFactory>(state);
            });

        TProgramBuilder pb(Env_, *FunctionRegistry_);
        auto* uint64Type = pb.NewDataType(NUdf::EDataSlot::Uint64);
        auto* streamRowType = pb.NewStructType({
            {"key", uint64Type},
            {"stream_value", uint64Type},
        });
        auto* lookupRowType = pb.NewStructType({
            {"key", uint64Type},
            {"lookup_value", uint64Type},
        });
        OutputRowType_ = AS_TYPE(TStructType, pb.NewStructType({
                                                  {"l.lookup_value", uint64Type},
                                                  {"s.key", uint64Type},
                                                  {"s.stream_value", uint64Type},
                                              }));

        TVector<TRuntimeNode> streamRows;
        for (ui64 key : {1, 2}) {
            streamRows.push_back(pb.NewStruct(streamRowType, {
                                                                 {"key", pb.NewDataLiteral<ui64>(key)},
                                                                 {"stream_value", pb.NewDataLiteral<ui64>(key * 10)},
                                                             }));
        }

        auto stream = pb.Iterator(
            pb.NewList(streamRowType, std::move(streamRows)),
            {});
        auto wrappedLookupSourceType = pb.Nop(
            pb.NewVoid(),
            pb.NewListType(lookupRowType));
        auto lookupSourceArgs = pb.NewTuple({
            pb.NewDataLiteral<NUdf::EDataSlot::String>("fake"),
            pb.NewVoid(),
        });
        auto joinKind = pb.NewDataLiteral<ui32>(
            static_cast<ui32>(EJoinKind::Inner));

        auto buildScope = [&](TStringBuf label, TStringBuf side) {
            return pb.NewTuple({
                pb.NewDataLiteral<NUdf::EDataSlot::String>(label),
                pb.NewDataLiteral<NUdf::EDataSlot::String>(side),
                pb.NewTuple({
                    pb.NewDataLiteral<NUdf::EDataSlot::String>("key"),
                }),
                pb.NewDataLiteral<ui32>(
                    static_cast<ui32>(ERowSelectionMode::Any)),
            });
        };

        auto inflightRowLimit = pb.NewDataLiteral<ui64>(1);
        auto inflightLookupLimit = pb.NewDataLiteral<ui64>(1);
        auto lookupTimeoutMs = pb.NewDataLiteral<ui64>(timeout.MilliSeconds());
        auto settings = pb.NewTuple({
            inflightRowLimit,
            inflightLookupLimit,
            lookupTimeoutMs,
        });

        TCallableBuilder call(
            Env_,
            "YtflowLookupJoin",
            pb.NewStreamType(OutputRowType_));
        call.Add(stream);
        call.Add(wrappedLookupSourceType);
        call.Add(lookupSourceArgs);
        call.Add(joinKind);
        call.Add(buildScope("s", "left"));
        call.Add(buildScope("l", "right"));
        call.Add(settings);
        Root_ = TRuntimeNode(call.Build(), /*isImmediate*/ false);

        Explorer_.Walk(Root_.GetNode(), Env_.GetNodeStack());
        auto nodeFactory = GetCompositeWithBuiltinFactory({
            [this](
                TCallable& callable,
                const TComputationNodeFactoryContext& ctx)
            {
                if (callable.GetType()->GetName() == "YtflowLookupJoin") {
                    return WrapYtflowLookupJoin(
                        callable,
                        ctx,
                        *LookupProviderRegistry_);
                }
                return static_cast<IComputationNode*>(nullptr);
            },
        });
        TComputationPatternOpts patternOpts(
            Alloc_.Ref(),
            Env_,
            std::move(nodeFactory),
            FunctionRegistry_.Get(),
            NUdf::EValidateMode::None,
            NUdf::EValidatePolicy::Fail,
            /*optLLVM*/ "",
            EGraphPerProcess::Multi,
            /*stats*/ nullptr,
            /*countersProvider*/ nullptr,
            /*secureParamsProvider*/ nullptr,
            /*logProvider*/ nullptr,
            UnknownLangVersion,
            RuntimeSettings_);
        Pattern_ = MakeComputationPattern(
            Explorer_,
            Root_,
            {Root_.GetNode()},
            patternOpts);

        LookupValueIndex_ = OutputRowType_->GetMemberIndex(
            "l.lookup_value");
        StreamKeyIndex_ = OutputRowType_->GetMemberIndex("s.key");
        Alloc_.Release();
    }

    ~TLookupJoinPattern()
    {
        Alloc_.Acquire();
        Pattern_.Reset();
        Root_ = {};
    }

    IComputationPattern& GetPattern() const {
        return *Pattern_;
    }

    bool GetSuitableForCache() const {
        return Pattern_->GetSuitableForCache();
    }

    const std::shared_ptr<TFakeLookupState>& GetState() const {
        return State_;
    }

    ui32 GetLookupValueIndex() const {
        return LookupValueIndex_;
    }

    ui32 GetStreamKeyIndex() const {
        return StreamKeyIndex_;
    }

private:
    TScopedAlloc Alloc_;
    TTypeEnvironment Env_;
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry_;
    TRuntimeSettings::TConstPtr RuntimeSettings_;
    std::shared_ptr<TFakeLookupState> State_;
    THolder<IYtflowLookupProviderRegistry> LookupProviderRegistry_;
    TExploringNodeVisitor Explorer_;
    IComputationPattern::TPtr Pattern_;
    TRuntimeNode Root_;
    TStructType* OutputRowType_ = nullptr;
    ui32 LookupValueIndex_ = 0;
    ui32 StreamKeyIndex_ = 0;
};

struct TFetchedRow {
    NUdf::EFetchStatus Status;
    ui64 StreamKey = 0;
    ui64 LookupValue = 0;
};

class TLookupJoinClone {
public:
    explicit TLookupJoinClone(TLookupJoinPattern& pattern)
        : Alloc_(__LOCATION__)
        , Env_(Alloc_)
        , RandomProvider_(CreateDeterministicRandomProvider(/*seed*/ 1))
        , TimeProvider_(CreateDeterministicTimeProvider(/*seed*/ 1))
        , RuntimeSettings_(MakeRuntimeSettings())
        , LookupValueIndex_(pattern.GetLookupValueIndex())
        , StreamKeyIndex_(pattern.GetStreamKeyIndex())
    {
        const TComputationOptsFull opts(
            /*stats*/ nullptr,
            Alloc_.Ref(),
            Env_,
            *RandomProvider_,
            *TimeProvider_,
            NUdf::EValidatePolicy::Fail,
            /*secureParamsProvider*/ nullptr,
            /*countersProvider*/ nullptr,
            /*logProvider*/ nullptr,
            UnknownLangVersion,
            RuntimeSettings_,
            /*bridgeMode*/ NUdf::EBridgeMode::None,
            /*bridgeBinaryPath*/ TString());
        Graph_ = pattern.GetPattern().Clone(opts);
        Graph_->Prepare();
        Stream_ = Graph_->GetValue();
        ValueBuilder_ = Graph_->GetContext().Builder;
        Alloc_.Release();
    }

    ~TLookupJoinClone()
    {
        DestroyGraph();
        Alloc_.Acquire();
    }

    TFetchedRow Fetch()
    {
        auto guard = Guard(Alloc_);
        NUdf::TUnboxedValue row;
        auto status = Stream_.Fetch(row);
        if (status != NUdf::EFetchStatus::Ok) {
            return {.Status = status};
        }

        return {
            .Status = status,
            .StreamKey = row.GetElement(StreamKeyIndex_).Get<ui64>(),
            .LookupValue = row.GetElement(LookupValueIndex_).Get<ui64>(),
        };
    }

    const NUdf::IValueBuilder* GetValueBuilder() const {
        return ValueBuilder_;
    }

    void DestroyGraph()
    {
        if (!Graph_) {
            return;
        }

        Alloc_.Acquire();
        Stream_ = {};
        Graph_.Reset();
        Alloc_.Release();
    }

private:
    TScopedAlloc Alloc_;
    TTypeEnvironment Env_;
    TIntrusivePtr<IRandomProvider> RandomProvider_;
    TIntrusivePtr<ITimeProvider> TimeProvider_;
    TRuntimeSettings::TConstPtr RuntimeSettings_;
    THolder<IComputationGraph> Graph_;
    NUdf::TUnboxedValue Stream_;
    const NUdf::IValueBuilder* ValueBuilder_ = nullptr;
    const ui32 LookupValueIndex_;
    const ui32 StreamKeyIndex_;
};

TEST(TLookupJoinGraphLocalStateTest, ClonesHaveIndependentState)
{
    TLookupJoinPattern pattern(
        ELookupMode::Ready,
        TDuration::Seconds(1));
    TLookupJoinClone first(pattern);
    TLookupJoinClone second(pattern);

    ASSERT_TRUE(pattern.GetSuitableForCache());
    const auto probes = pattern.GetState()->GetProbes();
    ASSERT_EQ(2, probes.size());
    ASSERT_NE(probes[0].ProviderId, probes[1].ProviderId);
    ASSERT_NE(
        probes[0].ProviderIdentity,
        probes[1].ProviderIdentity);
    ASSERT_NE(
        probes[0].CodecIdentity,
        probes[1].CodecIdentity);
    ASSERT_NE(
        probes[0].BufferIdentity,
        probes[1].BufferIdentity);
    ASSERT_NE(probes[0].ValueBuilder, probes[1].ValueBuilder);
    ASSERT_EQ(first.GetValueBuilder(), probes[0].ValueBuilder);
    ASSERT_EQ(second.GetValueBuilder(), probes[1].ValueBuilder);

    const auto firstRow1 = first.Fetch();
    const auto secondRow1 = second.Fetch();
    const auto firstRow2 = first.Fetch();
    const auto secondRow2 = second.Fetch();

    ASSERT_EQ(NUdf::EFetchStatus::Ok, firstRow1.Status);
    ASSERT_EQ(NUdf::EFetchStatus::Ok, secondRow1.Status);
    ASSERT_EQ(NUdf::EFetchStatus::Ok, firstRow2.Status);
    ASSERT_EQ(NUdf::EFetchStatus::Ok, secondRow2.Status);
    ASSERT_EQ(1, firstRow1.StreamKey);
    ASSERT_EQ(1, secondRow1.StreamKey);
    ASSERT_EQ(2, firstRow2.StreamKey);
    ASSERT_EQ(2, secondRow2.StreamKey);
    ASSERT_EQ(
        probes[0].ProviderId * 100 + 1,
        firstRow1.LookupValue);
    ASSERT_EQ(
        probes[1].ProviderId * 100 + 1,
        secondRow1.LookupValue);
    ASSERT_EQ(
        probes[0].ProviderId * 100 + 2,
        firstRow2.LookupValue);
    ASSERT_EQ(
        probes[1].ProviderId * 100 + 2,
        secondRow2.LookupValue);
    ASSERT_EQ(
        NUdf::EFetchStatus::Finish,
        first.Fetch().Status);
    ASSERT_EQ(
        NUdf::EFetchStatus::Finish,
        second.Fetch().Status);
    const auto completedProbes = pattern.GetState()->GetProbes();
    ASSERT_EQ(2, completedProbes[0].LookupCount);
    ASSERT_EQ(2, completedProbes[1].LookupCount);
    ASSERT_EQ(2, completedProbes[0].DecodeCount);
    ASSERT_EQ(2, completedProbes[1].DecodeCount);
}

TEST(TLookupJoinGraphLocalStateTest, PendingLookupsOutliveGraphs)
{
    TLookupJoinPattern pattern(
        ELookupMode::Controlled,
        TDuration::Zero());
    TLookupJoinClone first(pattern);
    TLookupJoinClone second(pattern);

    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        first.Fetch(),
        yexception,
        "Lookup timeout exceeded for table fake.table");
    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        second.Fetch(),
        yexception,
        "Lookup timeout exceeded for table fake.table");

    auto state = pattern.GetState();
    auto firstPendingPromise = state->ExtractPromise(0);
    auto secondPendingPromise = state->ExtractPromise(1);
    first.DestroyGraph();
    second.DestroyGraph();
    auto probes = state->GetProbes();
    ASSERT_EQ(2, probes.size());
    ASSERT_TRUE(probes[0].Destroyed);
    ASSERT_TRUE(probes[1].Destroyed);
    ASSERT_EQ(1, probes[0].LookupCount);
    ASSERT_EQ(1, probes[1].LookupCount);
    ASSERT_EQ(0, probes[0].DecodeCount);
    ASSERT_EQ(0, probes[1].DecodeCount);

    std::thread firstCompletionThread(
        [promise = std::move(firstPendingPromise)]() mutable {
            IYtflowLookupProvider::ILookupResultPtr result =
                std::make_shared<TFakeLookupResult>(1);
            promise.SetValue(std::move(result));
        });
    std::thread secondCompletionThread(
        [promise = std::move(secondPendingPromise)]() mutable {
            IYtflowLookupProvider::ILookupResultPtr result =
                std::make_shared<TFakeLookupResult>(2);
            promise.SetValue(std::move(result));
        });
    firstCompletionThread.join();
    secondCompletionThread.join();
    auto firstCompletionProbe = state->GetCompletionProbe(0);
    auto secondCompletionProbe = state->GetCompletionProbe(1);
    ASSERT_TRUE(
        firstCompletionProbe->Wait(TDuration::Seconds(1)));
    ASSERT_TRUE(
        secondCompletionProbe->Wait(TDuration::Seconds(1)));
    ASSERT_EQ(1, firstCompletionProbe->GetCount());
    ASSERT_EQ(1, secondCompletionProbe->GetCount());
    probes = state->GetProbes();
    ASSERT_TRUE(probes[0].Destroyed);
    ASSERT_TRUE(probes[1].Destroyed);
    ASSERT_EQ(0, probes[0].DecodeCount);
    ASSERT_EQ(0, probes[1].DecodeCount);
}

} // namespace
} // namespace NYql
