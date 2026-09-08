#pragma once

#include "yql_ytflow_function_registry.h"
#include "yql_ytflow_node_factory.h"

#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <yt/yql/providers/ytflow/integration/mkql_interface/yql_ytflow_lookup_provider.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <memory>

namespace NYql::NYtflow {

enum class EComputationPatternUnsuitabilityReason {
    YtflowCallableDenied /* "ytflow_callable_denied" */,
    UnknownYtflowCallable /* "unknown_ytflow_callable" */,
    MiniKqlPatternNotSuitable /* "minikql_pattern_not_suitable" */,
};

TMaybe<EComputationPatternUnsuitabilityReason> GetPatternUnsuitabilityReason(
    const TYtflowPatternMetadata& ytflowPatternMetadata,
    bool miniKqlPatternSuitable);

// Owns a reusable computation pattern together with every object referenced by
// it, allowing per-execution graphs to safely share the pattern.
class TComputationPatternHolder
    : public TThrRefBase {
public:
    ~TComputationPatternHolder();

    const NKikimr::NMiniKQL::IFunctionRegistry& GetFunctionRegistry() const;
    const THashMap<TString, const NKikimr::NMiniKQL::TType*>& GetInputTypes() const;
    const THashMap<TString, NKikimr::NMiniKQL::IComputationExternalNode*>& GetYtflowInputNodes() const;
    const NKikimr::NMiniKQL::TType* GetOutputType() const;
    bool GetSuitableForCache() const;
    const TMaybe<EComputationPatternUnsuitabilityReason>& GetUnsuitabilityReason() const;
    const TYtflowPatternMetadata& GetYtflowPatternMetadata() const;
    size_t GetLambdaFileBytes() const;
    size_t GetNodeCount() const;

private:
    friend TIntrusivePtr<TComputationPatternHolder> BuildComputationPatternHolder(
        TString lambdaFile,
        TIntrusivePtr<TFunctionRegistryHolder> functionRegistryHolder,
        TLangVersion langVersion,
        TString optLLVM,
        TRuntimeSettings::TConstPtr runtimeSettings);

    friend THolder<NKikimr::NMiniKQL::IComputationGraph> CloneComputationGraph(
        TIntrusivePtr<TComputationPatternHolder> patternHolder,
        NKikimr::NMiniKQL::TScopedAlloc& alloc,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        IRandomProvider& randomProvider,
        ITimeProvider& timeProvider);

    TComputationPatternHolder(
        TString lambdaFile,
        TIntrusivePtr<TFunctionRegistryHolder> functionRegistryHolder,
        TLangVersion langVersion,
        TString optLLVM,
        TRuntimeSettings::TConstPtr runtimeSettings);

private:
    TIntrusivePtr<TFunctionRegistryHolder> FunctionRegistryHolder_;
    NKikimr::NMiniKQL::TScopedAlloc PatternAlloc_;
    THolder<NKikimr::NMiniKQL::TTypeEnvironment> PatternEnv_;
    std::unique_ptr<NUdf::ISecureParamsProvider> SecureParamsProvider_;
    TLangVersion LangVersion_;
    TRuntimeSettings::TConstPtr RuntimeSettings_;
    THolder<IYtflowLookupProviderRegistry> YtflowLookupProviderRegistry_;
    NKikimr::NMiniKQL::TRuntimeNode Program_;
    THashMap<TString, const NKikimr::NMiniKQL::TType*> InputTypes_;
    THashMap<TString, NKikimr::NMiniKQL::IComputationExternalNode*> YtflowInputNodes_;
    const NKikimr::NMiniKQL::TType* OutputType_ = nullptr;
    NKikimr::NMiniKQL::IComputationPattern::TPtr Pattern_;
    TMaybe<EComputationPatternUnsuitabilityReason> UnsuitabilityReason_;
    TYtflowPatternMetadata YtflowPatternMetadata_;
    size_t LambdaFileBytes_ = 0;
    size_t NodeCount_ = 0;
};

TIntrusivePtr<TComputationPatternHolder> BuildComputationPatternHolder(
    TString lambdaFile,
    TIntrusivePtr<TFunctionRegistryHolder> functionRegistryHolder,
    TLangVersion langVersion,
    TString optLLVM,
    TRuntimeSettings::TConstPtr runtimeSettings);

// Clone-specific arguments are borrowed until the returned graph is destroyed;
// the caller must also acquire alloc before destroying the graph.
THolder<NKikimr::NMiniKQL::IComputationGraph> CloneComputationGraph(
    TIntrusivePtr<TComputationPatternHolder> patternHolder,
    NKikimr::NMiniKQL::TScopedAlloc& alloc,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
    IRandomProvider& randomProvider,
    ITimeProvider& timeProvider);

} // namespace NYql::NYtflow
