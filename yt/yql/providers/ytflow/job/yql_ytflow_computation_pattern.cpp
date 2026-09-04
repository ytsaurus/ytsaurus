#include "yql_ytflow_computation_pattern.h"

#include "yql_ytflow_node_factory.h"
#include "yql_ytflow_secure_params.h"

#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yql/providers/yt/mkql_ytflow/yql_yt_ytflow_lookup_provider.h>

#include <util/stream/file.h>

namespace NYql::NYtflow {
namespace {

class TComputationGraphWithPatternHolder final
    : public NKikimr::NMiniKQL::IComputationGraph {
public:
    TComputationGraphWithPatternHolder(
        TIntrusivePtr<TComputationPatternHolder> patternHolder,
        THolder<NKikimr::NMiniKQL::IComputationGraph> graph)
        : PatternHolder_(std::move(patternHolder))
        , Graph_(std::move(graph))
    {
    }

    void Prepare() override {
        Graph_->Prepare();
    }

    NUdf::TUnboxedValue GetValue() override {
        return Graph_->GetValue();
    }

    NKikimr::NMiniKQL::TComputationContext& GetContext() override {
        return Graph_->GetContext();
    }

    NKikimr::NMiniKQL::IComputationExternalNode* GetEntryPoint(size_t index, bool require) override {
        return Graph_->GetEntryPoint(index, require);
    }

    const NKikimr::NMiniKQL::TArrowKernelsTopology* GetKernelsTopology() override {
        return Graph_->GetKernelsTopology();
    }

    const NKikimr::NMiniKQL::TComputationNodePtrDeque& GetNodes() const override {
        return Graph_->GetNodes();
    }

    void Invalidate() override {
        Graph_->Invalidate();
    }

    void InvalidateCaches() override {
        Graph_->InvalidateCaches();
    }

    NKikimr::NMiniKQL::TMemoryUsageInfo& GetMemInfo() const override {
        return Graph_->GetMemInfo();
    }

    const NKikimr::NMiniKQL::THolderFactory& GetHolderFactory() const override {
        return Graph_->GetHolderFactory();
    }

    NKikimr::NMiniKQL::ITerminator* GetTerminator() const override {
        return Graph_->GetTerminator();
    }

    bool SetExecuteLLVM(bool value) override {
        return Graph_->SetExecuteLLVM(value);
    }

    TString SaveGraphState() override {
        return Graph_->SaveGraphState();
    }

    void LoadGraphState(TStringBuf state) override {
        Graph_->LoadGraphState(state);
    }

    TMaybe<NUdf::TSourcePosition> GetNotConsumedLinear() override {
        return Graph_->GetNotConsumedLinear();
    }

    bool GetFlushingMode() const override {
        return Graph_->GetFlushingMode();
    }

    void SetFlushingMode(bool value) override {
        Graph_->SetFlushingMode(value);
    }

private:
    // The proxy retains PatternHolder_ to keep the shared pattern alive for the
    // entire lifetime of the cloned graph. Graph_ is destroyed first and can
    // safely refer to PatternHolder_ during its entire lifetime.
    TIntrusivePtr<TComputationPatternHolder> PatternHolder_;
    THolder<NKikimr::NMiniKQL::IComputationGraph> Graph_;
};

} // namespace

TMaybe<EComputationPatternUnsuitabilityReason> GetPatternUnsuitabilityReason(
    const TYtflowPatternMetadata& ytflowPatternMetadata,
    bool miniKqlPatternSuitable)
{
    bool hasPrivateOnlyCallable = false;
    for (const auto& [_, sharing] : ytflowPatternMetadata.SpecializedCallables) {
        if (sharing == EYtflowCallablePatternSharing::Unknown) {
            return EComputationPatternUnsuitabilityReason::UnknownYtflowCallable;
        }

        hasPrivateOnlyCallable |=
            sharing == EYtflowCallablePatternSharing::PrivateOnly;
    }

    if (hasPrivateOnlyCallable) {
        return EComputationPatternUnsuitabilityReason::YtflowCallableDenied;
    }

    if (!miniKqlPatternSuitable) {
        return EComputationPatternUnsuitabilityReason::MiniKqlPatternNotSuitable;
    }

    return Nothing();
}

TComputationPatternHolder::TComputationPatternHolder(
    TString lambdaFile,
    TIntrusivePtr<TFunctionRegistryHolder> functionRegistryHolder,
    TLangVersion langVersion,
    TString optLLVM,
    TRuntimeSettings::TConstPtr runtimeSettings)
    : FunctionRegistryHolder_(std::move(functionRegistryHolder))
    , PatternAlloc_(__LOCATION__)
    , LangVersion_(langVersion)
    , RuntimeSettings_(std::move(runtimeSettings))
{
    PatternAlloc_.Ref().UseRefLocking = true;
    PatternEnv_ = MakeHolder<NKikimr::NMiniKQL::TTypeEnvironment>(PatternAlloc_);

    SecureParamsProvider_ = CreateSecureParamsProvider();
    YtflowLookupProviderRegistry_ = CreateYtflowLookupProviderRegistry();
    RegisterYtYtflowLookupProvider(*YtflowLookupProviderRegistry_);

    const auto serializedProgram = TFileInput(std::move(lambdaFile)).ReadAll();
    LambdaFileBytes_ = serializedProgram.size();
    TNodeFactoryResult nodeFactoryResult;
    Program_ = NKikimr::NMiniKQL::DeserializeRuntimeNode(
        serializedProgram,
        *PatternEnv_);

    NKikimr::NMiniKQL::TExploringNodeVisitor explorer;
    explorer.Walk(Program_.GetNode(), PatternEnv_->GetNodeStack());
    NodeCount_ = explorer.GetNodes().size();

    const auto nodeFactoryMetadata = TNodeFactoryMetadata{
        .YtflowLookupProviderRegistry = *YtflowLookupProviderRegistry_,
    };
    auto compositeNodeFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
        GetNodeFactory(nodeFactoryMetadata, nodeFactoryResult),
    });

    NKikimr::NMiniKQL::TComputationPatternOpts patternOpts(
        PatternAlloc_.Ref(),
        *PatternEnv_,
        std::move(compositeNodeFactory),
        &FunctionRegistryHolder_->GetFunctionRegistry(),
        NUdf::EValidateMode::None,
        NUdf::EValidatePolicy::Fail,
        std::move(optLLVM),
        NKikimr::NMiniKQL::EGraphPerProcess::Multi,
        /*stats*/ nullptr,
        /*countersProvider*/ nullptr,
        SecureParamsProvider_.get(),
        /*logProvider*/ nullptr,
        LangVersion_,
        RuntimeSettings_);

    Pattern_ = NKikimr::NMiniKQL::MakeComputationPattern(
        explorer,
        Program_,
        {Program_.GetNode()},
        patternOpts);

    YtflowPatternMetadata_ = std::move(nodeFactoryResult.PatternMetadata);
    UnsuitabilityReason_ = GetPatternUnsuitabilityReason(
        YtflowPatternMetadata_,
        Pattern_->GetSuitableForCache());

    InputTypes_ = std::move(nodeFactoryResult.InputTypes);
    YtflowInputNodes_ = std::move(nodeFactoryResult.YtflowInputNodes);
    OutputType_ = Program_.GetStaticType();

    PatternAlloc_.Release();
}

TComputationPatternHolder::~TComputationPatternHolder()
{
    PatternAlloc_.Acquire();
    Pattern_.Reset();
}

const NKikimr::NMiniKQL::IFunctionRegistry& TComputationPatternHolder::GetFunctionRegistry() const {
    return FunctionRegistryHolder_->GetFunctionRegistry();
}

const THashMap<TString, const NKikimr::NMiniKQL::TType*>& TComputationPatternHolder::GetInputTypes() const {
    return InputTypes_;
}

const THashMap<TString, NKikimr::NMiniKQL::IComputationExternalNode*>& TComputationPatternHolder::GetYtflowInputNodes() const {
    return YtflowInputNodes_;
}

const NKikimr::NMiniKQL::TType* TComputationPatternHolder::GetOutputType() const {
    return OutputType_;
}

bool TComputationPatternHolder::GetSuitableForCache() const {
    return !UnsuitabilityReason_;
}

const TMaybe<EComputationPatternUnsuitabilityReason>& TComputationPatternHolder::GetUnsuitabilityReason() const {
    return UnsuitabilityReason_;
}

const TYtflowPatternMetadata& TComputationPatternHolder::GetYtflowPatternMetadata() const {
    return YtflowPatternMetadata_;
}

size_t TComputationPatternHolder::GetLambdaFileBytes() const {
    return LambdaFileBytes_;
}

size_t TComputationPatternHolder::GetNodeCount() const {
    return NodeCount_;
}

TIntrusivePtr<TComputationPatternHolder> BuildComputationPatternHolder(
    TString lambdaFile,
    TIntrusivePtr<TFunctionRegistryHolder> functionRegistryHolder,
    TLangVersion langVersion,
    TString optLLVM,
    TRuntimeSettings::TConstPtr runtimeSettings)
{
    return new TComputationPatternHolder(
        std::move(lambdaFile),
        std::move(functionRegistryHolder),
        langVersion,
        std::move(optLLVM),
        std::move(runtimeSettings));
}

THolder<NKikimr::NMiniKQL::IComputationGraph> CloneComputationGraph(
    TIntrusivePtr<TComputationPatternHolder> patternHolder,
    NKikimr::NMiniKQL::TScopedAlloc& alloc,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    IRandomProvider& randomProvider,
    ITimeProvider& timeProvider)
{
    YQL_ENSURE(patternHolder->Pattern_, "Computation pattern is not available");

    const NKikimr::NMiniKQL::TComputationOptsFull computationOpts(
        /*stats*/ nullptr,
        alloc.Ref(),
        typeEnv,
        randomProvider,
        timeProvider,
        NUdf::EValidatePolicy::Fail,
        patternHolder->SecureParamsProvider_.get(),
        /*countersProvider*/ nullptr,
        /*logProvider*/ nullptr,
        patternHolder->LangVersion_,
        patternHolder->RuntimeSettings_,
        /*bridgeMode*/ NUdf::EBridgeMode::None,
        /*bridgeBinaryPath*/ TString());

    auto graph = patternHolder->Pattern_->Clone(computationOpts);
    return MakeHolder<TComputationGraphWithPatternHolder>(
        std::move(patternHolder),
        std::move(graph));
}

} // namespace NYql::NYtflow
