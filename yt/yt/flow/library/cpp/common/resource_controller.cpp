#include "resource_controller.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TNullResourceController::TNullResourceController(
    TResourceControllerContextPtr /*context*/,
    TDynamicResourceControllerContextPtr /*dynamicContext*/)
{ }

void TNullResourceController::Init(IInitContextPtr /*initContext*/)
{ }

NYTree::INodePtr TNullResourceController::BuildTargetRevisionSpec()
{
    return nullptr;
}

void TNullResourceController::CollectStatuses(
    const THashMap<std::string, TWorkerResourceStatusPtr>& /*workerStatuses*/,
    const TWorkerResourceStatusPtr& /*controllerStatus*/)
{ }

NYTree::IMapNodePtr TNullResourceController::GetView()
{
    return nullptr;
}

IResourceController::TParametersPtr TNullResourceController::GetParametersBase() const
{
    return nullptr;
}

IResourceController::TDynamicParametersPtr TNullResourceController::GetDynamicParametersBase() const
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
