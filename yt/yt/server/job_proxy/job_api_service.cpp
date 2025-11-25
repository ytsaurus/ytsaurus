#include "job_api_service.h"

#include <yt/yt/server/lib/job_proxy/config.h>
#include <yt/yt/server/lib/job_proxy/job_api_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, JobApiLogger, "JobApi");

////////////////////////////////////////////////////////////////////////////////

class TJobApiService
    : public NRpc::TServiceBase
{
public:
    TJobApiService(
        TJobApiServiceConfigPtr config,
        IInvokerPtr controlInvoker)
        : NRpc::TServiceBase(
            std::move(controlInvoker),
            TJobApiServiceProxy::GetDescriptor(),
            JobApiLogger())
        , Config_(std::move(config))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProgressSaved));
    }

private:
    TJobApiServiceConfigPtr Config_;

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::JobApi::NProto, ProgressSaved)
    {
        YT_LOG_INFO("Received a snapshot notification");
        context->SetRequestInfo();
        context->Reply();
    }
};

NRpc::IServicePtr CreateJobApiService(
    NJobProxy::TJobApiServiceConfigPtr config,
    IInvokerPtr controlInvoker)
{
    return New<TJobApiService>(
        std::move(config),
        std::move(controlInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
