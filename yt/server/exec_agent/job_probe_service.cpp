#include "stdafx.h"
#include "job_probe_service.h"
#include "private.h"

#include <server/cell_node/bootstrap.h>

#include <server/job_agent/job_controller.h>

#include <ytlib/job_probe_client/job_probe_service_proxy.h>

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NExecAgent {

using namespace NRpc;
using namespace NJobProbeClient;
using namespace NConcurrency;
using namespace NJobAgent;

////////////////////////////////////////////////////////////////////

class TJobProbeService
	: public TServiceBase
{
public:
    TJobProbeService(NCellNode::TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TJobProbeServiceProxy::GetServiceName(),
            ExecAgentLogger,
            TJobProbeServiceProxy::GetProtocolVersion())
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateInputContext));
    }

private:
	NCellNode::TBootstrap* Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NJobProbeClient::NProto, GenerateInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        auto chunkIds = job->GetInputContexts();

        context->SetResponseInfo("ChunkIds: %v", JoinToString(chunkIds));
        ToProto(response->mutable_chunk_id(), chunkIds);
        context->Reply();
    }
};

IServicePtr CreateJobProbeService(NCellNode::TBootstrap* bootstrap)
{
    return New<TJobProbeService>(bootstrap);
}

////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT