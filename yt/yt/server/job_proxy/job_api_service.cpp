#include "job_api_service.h"

#include "job_proxy.h"

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
        IInvokerPtr controlInvoker,
        TWeakPtr<TJobProxy> jobProxy)
        : NRpc::TServiceBase(
            std::move(controlInvoker),
            TJobApiServiceProxy::GetDescriptor(),
            JobApiLogger())
        , Config_(std::move(config))
        , JobProxy_(jobProxy)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProgressSaved));
    }

private:
    TJobApiServiceConfigPtr Config_;
    TWeakPtr<TJobProxy> JobProxy_;

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::JobApi::NProto, ProgressSaved)
    {
        YT_LOG_INFO("Received a progress save notification");

        TInstant progressSaveTime = TInstant::Now();
        GetJobProxy()->OnProgressSaved(progressSaveTime);

        context->SetRequestInfo();
        context->Reply();
    }

    TJobProxyPtr GetJobProxy()
    {
        auto jobProxy = JobProxy_.Lock();
        if (!jobProxy) {
            THROW_ERROR_EXCEPTION("Job proxy is dead");
        }
        return jobProxy;
    }
};

NRpc::IServicePtr CreateJobApiService(
    NJobProxy::TJobApiServiceConfigPtr config,
    IInvokerPtr controlInvoker,
    TWeakPtr<TJobProxy> jobProxy)
{
    return New<TJobApiService>(
        std::move(config),
        std::move(controlInvoker),
        std::move(jobProxy));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
