#include "resource_controller.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void IResourceController::Stop(const TError& error)
{
    GetCancelableContext()->Cancel(error);
}

std::optional<TError> IResourceController::GetStatus() const
{
    const auto& context = GetCancelableContext();
    if (context->IsCanceled()) {
        return context->GetCancelationError();
    }
    return std::nullopt;
}

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

const TCancelableContextPtr& TNullResourceController::GetCancelableContext() const
{
    return CancelableContext_;
}

void TNullResourceController::OnReconfigureFail(const TError& error)
{
    Stop(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
