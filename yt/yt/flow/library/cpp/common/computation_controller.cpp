#include "computation_controller.h"

namespace NYT::NFlow {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

IClientPtr TComputationControllerContextBase::GetClient() const
{
    THROW_ERROR_EXCEPTION_UNLESS(PipelinePath.GetCluster().has_value(), "Pipeline path must have cluster");
    return ClientsCache->GetClient(*PipelinePath.GetCluster());
}

IResourcePtr TComputationControllerContextBase::GetStaticResource(const char resourceId[])
{
    return GetStaticResource(TResourceId(resourceId));
}

IResourcePtr TComputationControllerContextBase::GetStaticResource(const TResourceId& resourceId)
{
    auto resource = GetOrDefault(StaticResources, resourceId);
    if (!resource) {
        THROW_ERROR_EXCEPTION("Static resource is not found")
            << TErrorAttribute("resource_id", resourceId);
    }
    return resource;
}

////////////////////////////////////////////////////////////////////////////////

TComputationControllerContext::TComputationControllerContext(TComputationControllerCommonContextPtr commonContext)
    : CommonContext(std::move(commonContext))
{ }

////////////////////////////////////////////////////////////////////////////////

void IComputationController::TParametersBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void IComputationController::TDynamicParametersBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
