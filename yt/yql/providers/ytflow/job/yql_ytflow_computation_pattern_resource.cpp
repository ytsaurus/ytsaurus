#include "yql_ytflow_computation_pattern_resource.h"
#include "yql_ytflow_function_registry_resource.h"
#include "yql_ytflow_metrics.h"

#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/misc/error.h>

namespace NYql::NYtflow {

void TComputationPatternResourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("recipe_version", &TThis::RecipeVersion);
    registrar.Parameter("lambda_file", &TThis::LambdaFile);
    registrar.Parameter("lang_version", &TThis::LangVersion);
    registrar.Parameter("opt_llvm", &TThis::OptLLVM);
    registrar.Parameter("runtime_settings", &TThis::RuntimeSettings);
}

TComputationPatternResult TComputationPatternResult::Suitable(
    TIntrusivePtr<TComputationPatternHolder> patternHolder)
{
    return TComputationPatternResult(std::move(patternHolder));
}

TComputationPatternResult TComputationPatternResult::Unsuitable(
    EComputationPatternUnsuitabilityReason reason)
{
    return TComputationPatternResult(reason);
}

TComputationPatternResult::TComputationPatternResult(
    TIntrusivePtr<TComputationPatternHolder> patternHolder)
    : PatternHolder_(std::move(patternHolder))
{
    YT_VERIFY(PatternHolder_);
}

TComputationPatternResult::TComputationPatternResult(
    EComputationPatternUnsuitabilityReason reason)
    : UnsuitabilityReason_(reason)
{ }

bool TComputationPatternResult::IsSuitable() const {
    return static_cast<bool>(PatternHolder_);
}

const TIntrusivePtr<TComputationPatternHolder>& TComputationPatternResult::GetPatternHolder() const {
    YT_VERIFY(IsSuitable());
    return PatternHolder_;
}

EComputationPatternUnsuitabilityReason TComputationPatternResult::GetUnsuitabilityReason() const {
    YT_VERIFY(!IsSuitable());
    return *UnsuitabilityReason_;
}

TComputationPatternResource::TComputationPatternResource(
    NYT::NFlow::TResourceContextPtr context,
    NYT::NFlow::TDynamicResourceContextPtr dynamicContext)
    : TResourceBase(context, std::move(dynamicContext))
    , Metrics_(*context)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        GetParameters()->RecipeVersion == ComputationPatternResourceRecipeVersion,
        "Unsupported computation pattern recipe version %v; expected %v",
        GetParameters()->RecipeVersion,
        ComputationPatternResourceRecipeVersion);
}

NYT::TFuture<void> TComputationPatternResource::Load(
    const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& dependencies)
{
    auto loadGuard = Metrics_.ProfileLoad();

    auto functionRegistryHolder =
        dependencies.at(NYT::NFlow::TResourceId(FunctionRegistryDependencyAlias))
            ->As<TFunctionRegistryResource>()
            ->GetFunctionRegistryHolder();

    auto runtimeSettings =
        GetParameters()->RuntimeSettings.empty()
            ? MakeRuntimeSettings()
            : CreateRuntimeSettingsFromString(GetParameters()->RuntimeSettings);

    auto patternHolder =
        BuildComputationPatternHolder(
            GetParameters()->LambdaFile,
            functionRegistryHolder,
            GetParameters()->LangVersion,
            GetParameters()->OptLLVM,
            std::move(runtimeSettings));
    Metrics_.RecordShape(
        patternHolder->GetLambdaFileBytes(),
        patternHolder->GetNodeCount());

    if (patternHolder->GetSuitableForCache()) {
        Result_ = TComputationPatternResult::Suitable(std::move(patternHolder));
    } else {
        const auto reason = *patternHolder->GetUnsuitabilityReason();
        patternHolder.Reset();
        Result_ = TComputationPatternResult::Unsuitable(reason);
    }

    const auto& result = GetResult();
    Metrics_.RecordResult(result);
    if (result.IsSuitable()) {
        YT_TLOG_INFO("Loaded suitable YQL computation pattern resource")
            .With("ResourceIncarnationGeneration", GetContext()->ResourceIncarnationGeneration);
    } else {
        const auto reason = result.GetUnsuitabilityReason();
        YT_TLOG_INFO("Loaded unsuitable YQL computation pattern resource; using a private pattern")
            .With("ResourceIncarnationGeneration", GetContext()->ResourceIncarnationGeneration)
            .With("Reason", GetComputationPatternUnsuitabilityReasonName(reason));
    }

    return NYT::OKFuture;
}

const TComputationPatternResult& TComputationPatternResource::GetResult() const {
    YT_VERIFY(Result_);
    return *Result_;
}

} // namespace NYql::NYtflow
