#include "stdafx.h"
#include "job_probe_service.h"
#include "scheduler.h"
#include "private.h"

#include <server/cell_scheduler/bootstrap.h>

#include <ytlib/scheduler/job_probe_service_proxy.h>

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////

class TJobProbeService
	: public TServiceBase
{
public:
	TJobProbeService(TBootstrap* bootstrap)
		: TServiceBase(
			bootstrap->GetControlInvoker(),
            TJobProbeServiceProxy::GetServiceName(),
            SchedulerLogger,
            TJobProbeServiceProxy::GetProtocolVersion(),
            bootstrap->GetResponseKeeper())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateInputContext));
    }

private:
    TBootstrap* Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, GenerateInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        auto path = FromProto<Stroka>(request->path());
        context->SetRequestInfo("JobId: %v. Path: %v", jobId, path);

        WaitFor(Bootstrap_->GetScheduler()->GenerateInputContext(jobId, path))
            .ThrowOnError();

        context->Reply();
    }
};

IServicePtr CreateJobProbeService(TBootstrap* bootstrap)
{
	return New<TJobProbeService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT