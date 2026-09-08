#pragma once

#include "yql_ytflow_function_registry.h"
#include "yql_ytflow_metrics.h"

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql::NYtflow {

inline constexpr int FunctionRegistryResourceRecipeVersion = 1;

struct TFunctionRegistryResourceParameters
    : public NYT::NYTree::TYsonStruct {
    int RecipeVersion{};
    TVector<TString> UdfPaths;

    REGISTER_YSON_STRUCT(TFunctionRegistryResourceParameters);

    static void Register(TRegistrar registrar);
};

// Provides one immutable function registry shared by computations within a
// worker and PipelineSpec generation.
class TFunctionRegistryResource
    : public NYT::NFlow::TResourceBase {
public:
    YT_FLOW_EXTEND_PARAMETERS(TFunctionRegistryResourceParameters);

    TFunctionRegistryResource(
        NYT::NFlow::TResourceContextPtr context,
        NYT::NFlow::TDynamicResourceContextPtr dynamicContext);

    NYT::TFuture<void> Load(
        const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& dependencies) override;

    TIntrusivePtr<TFunctionRegistryHolder> GetFunctionRegistryHolder() const;

private:
    TFunctionRegistryMetrics Metrics_;
    TIntrusivePtr<TFunctionRegistryHolder> FunctionRegistryHolder_;
};

DEFINE_REFCOUNTED_TYPE(TFunctionRegistryResource);

} // namespace NYql::NYtflow
