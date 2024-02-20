#ifndef ERROR_REPORTING_SERVICE_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include error_reportion_service.h"
// For the sake of sane code completion.
#include "error_reporting_service_base.h"
#endif

#include "bootstrap.h"
#include "error_manager.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TBaseService>
template <class ...TArgs>
TErrorReportingServiceBase<TBaseService>::TErrorReportingServiceBase(
    IBootstrap* bootstrap,
    TArgs&&... args)
    : TBaseService(std::forward<TArgs>(args)...)
    , Bootstrap_(bootstrap)
{ }

template <class TBaseService>
void TErrorReportingServiceBase<TBaseService>::BeforeInvoke(NRpc::IServiceContext* /*context*/)
{
    ResetErrorManagerContext();
}

template <class TBaseService>
void TErrorReportingServiceBase<TBaseService>::OnMethodError(const TError& error, const TString& method)
{
    Bootstrap_->GetErrorManager()->HandleError(error, method);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
