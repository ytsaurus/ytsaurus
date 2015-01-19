#include "stdafx.h"
#include "job_probe_service.h"
#include "scheduler.h"
#include "private.h"

#include <server/cell_scheduler/bootstrap.h>

#include <ytlib/scheduler/job_probe_service_proxy.h>

#include <ytlib/job_probe_client/job_probe_service_proxy.h>

#include <ytlib/chunk_client/private.h>

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
        auto job = Bootstrap_->GetScheduler()->FindJob(jobId);

        if (job != nullptr) {
            const auto& address = job->GetNode()->Descriptor().GetDefaultAddress();
            auto channel = NChunkClient::LightNodeChannelFactory->CreateChannel(address);

            auto probeProxy = std::make_unique<NJobProbeClient::TJobProbeServiceProxy>(channel);

            auto req = probeProxy->GenerateInputContext();
            ToProto(req->mutable_job_id(), jobId);

            WaitFor(req->Invoke());
        }

        context->Reply();
    }
};

IServicePtr CreateJobProbeService(TBootstrap* bootstrap)
{
	return New<TJobProbeService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

}
}