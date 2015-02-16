#include "stdafx.h"
#include "job_prober_service.h"
#include "private.h"

#include <server/cell_node/bootstrap.h>

#include <server/job_agent/job_controller.h>

#include <ytlib/job_prober_client/job_prober_service_proxy.h>

#include <core/rpc/service_detail.h>

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
    }

private:
    NCellNode::TBootstrap* Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        auto chunkIds = job->DumpInputContexts();

        context->SetResponseInfo("ChunkIds: %v", JoinToString(chunkIds));
        ToProto(response->mutable_chunk_id(), chunkIds);
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
