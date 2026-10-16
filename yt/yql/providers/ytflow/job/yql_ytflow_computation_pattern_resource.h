#pragma once

#include "yql_ytflow_computation_pattern.h"
#include "yql_ytflow_metrics.h"

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NYql::NYtflow {

inline constexpr int ComputationPatternResourceRecipeVersion = 1;
inline constexpr TStringBuf FunctionRegistryDependencyAlias = "function_registry";
inline constexpr TStringBuf ComputationPatternResourceAlias = "computation_pattern";

struct TComputationPatternResourceParameters
    : public NYT::NYTree::TYsonStruct {
    int RecipeVersion{};
    TString LambdaFile;
    TLangVersion LangVersion;
    TString OptLLVM;
    TString RuntimeSettings;

    REGISTER_YSON_STRUCT(TComputationPatternResourceParameters);

    static void Register(TRegistrar registrar);
};

class TComputationPatternResult {
public:
    static TComputationPatternResult Suitable(
        TIntrusivePtr<TComputationPatternHolder> patternHolder);

    static TComputationPatternResult Unsuitable(
        EComputationPatternUnsuitabilityReason reason);

    bool IsSuitable() const;
    const TIntrusivePtr<TComputationPatternHolder>& GetPatternHolder() const;
    EComputationPatternUnsuitabilityReason GetUnsuitabilityReason() const;

private:
    explicit TComputationPatternResult(
        TIntrusivePtr<TComputationPatternHolder> patternHolder);

    explicit TComputationPatternResult(
        EComputationPatternUnsuitabilityReason reason);

    TIntrusivePtr<TComputationPatternHolder> PatternHolder_;
    TMaybe<EComputationPatternUnsuitabilityReason> UnsuitabilityReason_;
};

class TComputationPatternResource
    : public NYT::NFlow::TResourceBase {
public:
    YT_FLOW_EXTEND_PARAMETERS(TComputationPatternResourceParameters);

    TComputationPatternResource(
        NYT::NFlow::TResourceContextPtr context,
        NYT::NFlow::TDynamicResourceContextPtr dynamicContext);

    NYT::TFuture<void> Load(
        const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& dependencies) override;

    const TComputationPatternResult& GetResult() const;

private:
    TComputationPatternMetrics Metrics_;
    TMaybe<TComputationPatternResult> Result_;
};

DEFINE_REFCOUNTED_TYPE(TComputationPatternResource);

} // namespace NYql::NYtflow
