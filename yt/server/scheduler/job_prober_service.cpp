#include "stdafx.h"
#include "job_prober_service.h"
#include "scheduler.h"
#include "private.h"

#include <server/cell_scheduler/bootstrap.h>

#include <ytlib/scheduler/job_prober_service_proxy.h>

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NConcurrency;

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
    }

private:
    TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, DumpInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto path = FromProto<Stroka>(request->path());
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
};

IServicePtr CreateJobProberService(TBootstrap* bootstrap)
{
    return New<TJobProberService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
