#include "coverage_service.h"
#include "coverage_proxy.h"
#include "helpers.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NCoverage {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, CoverageLogger, "Coverage");

////////////////////////////////////////////////////////////////////////////////

class TCoverageService
    : public NRpc::TServiceBase
{
public:
    TCoverageService()
        : TServiceBase(
            TDispatcher::Get()->GetHeavyInvoker(),
            TCoverageProxy::GetDescriptor(),
            CoverageLogger())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Collect));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NProto, Collect);
};

DEFINE_RPC_SERVICE_METHOD(TCoverageService, Collect)
{
    context->SetRequestInfo();

    auto* result = response->mutable_coverage_map();
    ReadCoverageOrThrow(result);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateCoverageService()
{
    return New<TCoverageService>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoverage
