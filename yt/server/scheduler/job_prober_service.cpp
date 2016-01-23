#include "job_prober_service.h"
#include "private.h"
#include "scheduler.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/ytlib/job_prober_client/job_signal.h>

#include <yt/ytlib/scheduler/job_prober_service_proxy.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NConcurrency;
using namespace NJobProberClient;

////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    TJobProberService(TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TJobProberServiceProxy::GetServiceName(),
            SchedulerLogger,
            TJobProberServiceProxy::GetProtocolVersion())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Strace));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SignalJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbandonJob));
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, DumpInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        const auto& path = request->path();
        context->SetRequestInfo("JobId: %v, Path: %v",
            jobId,
            path);

        WaitFor(Bootstrap_->GetScheduler()->DumpInputContext(jobId, path))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Strace)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto trace = WaitFor(Bootstrap_->GetScheduler()->Strace(jobId))
            .ValueOrThrow();

        context->SetResponseInfo("Trace: %v", trace.Data());

        ToProto(response->mutable_trace(), trace.Data());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, SignalJob)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        const auto& signalName = request->signal_name();

        ValidateSignalName(request->signal_name());

        context->SetRequestInfo("JobId: %v, SignalName: %v",
            jobId,
            signalName);

        WaitFor(Bootstrap_->GetScheduler()->SignalJob(jobId, signalName))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, AbandonJob)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        WaitFor(Bootstrap_->GetScheduler()->AbandonJob(jobId))
            .ThrowOnError();

        context->Reply();
    }
};

IServicePtr CreateJobProberService(TBootstrap* bootstrap)
{
    return New<TJobProberService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
