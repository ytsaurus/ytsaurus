#include "resource_controller.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TNullResourceController::TNullResourceController(
    TResourceControllerContextPtr /*context*/,
    TDynamicResourceControllerContextPtr /*dynamicContext*/)
{ }

void TNullResourceController::Init(IInitContextPtr /*initContext*/)
{ }

TFuture<void> TNullResourceController::GetPreparingFuture()
{
    return OKFuture;
}

TResourceRevisionPtr TNullResourceController::BuildTargetRevision()
{
    return nullptr;
}

void TNullResourceController::CollectStatuses(
    const THashMap<std::string, TWorkerStatusPtr>& /*workerStatuses*/,
    const TWorkerResourceStatusPtr& /*controllerStatus*/,
    std::optional<i64> /*publishedRevisionId*/)
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
