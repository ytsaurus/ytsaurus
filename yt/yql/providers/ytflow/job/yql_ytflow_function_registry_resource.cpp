#include "yql_ytflow_function_registry_resource.h"

#include "yql_ytflow_metrics.h"

#include <yt/yt/core/misc/error.h>

namespace NYql::NYtflow {

void TFunctionRegistryResourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("recipe_version", &TThis::RecipeVersion);
    registrar.Parameter("udf_paths", &TThis::UdfPaths);
}

TFunctionRegistryResource::TFunctionRegistryResource(
    NYT::NFlow::TResourceContextPtr context,
    NYT::NFlow::TDynamicResourceContextPtr dynamicContext)
    : TResourceBase(context, std::move(dynamicContext))
    , Metrics_(context->Profiler)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        GetParameters()->RecipeVersion == FunctionRegistryResourceRecipeVersion,
        "Unsupported function registry recipe version %v; expected %v",
        GetParameters()->RecipeVersion,
        FunctionRegistryResourceRecipeVersion);
}

NYT::TFuture<void> TFunctionRegistryResource::Load(
    const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& /*dependencies*/)
{
    auto loadGuard = Metrics_.ProfileLoad();
    Metrics_.RecordUdfPaths(GetParameters()->UdfPaths);
    FunctionRegistryHolder_ = CreateFunctionRegistryHolder(GetParameters()->UdfPaths);
    return NYT::OKFuture;
}

TIntrusivePtr<TFunctionRegistryHolder> TFunctionRegistryResource::GetFunctionRegistryHolder() const {
    return FunctionRegistryHolder_;
}

} // namespace NYql::NYtflow
