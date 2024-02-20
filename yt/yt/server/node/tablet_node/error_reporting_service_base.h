
#pragma once

#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TBaseService>
class TErrorReportingServiceBase
    : public TBaseService
{
public:
    template <class... TArgs>
    TErrorReportingServiceBase(IBootstrap* bootstrap, TArgs&&... args);

    void BeforeInvoke(NRpc::IServiceContext* context) override;

    void OnMethodError(const TError& error, const TString& method) override;

private:
    IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define ERROR_REPORTING_SERVICE_BASE_INL_H_
#include "error_reporting_service_base-inl.h"
#undef ERROR_REPORTING_SERVICE_BASE_INL_H_
