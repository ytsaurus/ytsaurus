#include "job_prober_service.h"
#include "private.h"

#include <yt/server/cell_node/bootstrap.h>

#include <yt/server/job_agent/job_controller.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT {
namespace NExecAgent {

using namespace NRpc;
using namespace NJobProberClient;
using namespace NConcurrency;
using namespace NJobAgent;

////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    TJobProberService(NCellNode::TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TJobProberServiceProxy::GetServiceName(),
            ExecAgentLogger,
            TJobProberServiceProxy::GetProtocolVersion())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Strace));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SignalJob));
    }

private:
    NCellNode::TBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        auto chunkIds = job->DumpInputContexts();

        context->SetResponseInfo("ChunkIds: %v", chunkIds);
        ToProto(response->mutable_chunk_ids(), chunkIds);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Strace)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        auto trace = job->Strace();

        context->SetResponseInfo("Trace: %Qv", trace.Data());

        ToProto(response->mutable_trace(), trace.Data());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, SignalJob)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        const auto& signalName = request->signal_name();

        context->SetRequestInfo("JobId: %v, SignalName: %v",
            jobId,
            signalName);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        job->SignalJob(signalName);

        context->Reply();
    }
};

IServicePtr CreateJobProberService(NCellNode::TBootstrap* bootstrap)
{
    return New<TJobProberService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
