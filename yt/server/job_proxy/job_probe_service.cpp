#include "stdafx.h"
#include "job_probe_service.h"
#include "private.h"

#include "job_proxy.h"

#include <ytlib/job_probe_client/job_probe_service_proxy.h>

#include <ytlib/chunk_client/public.h>

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NJobProxy {

using namespace NRpc;
using namespace NJobProbeClient;
using namespace NConcurrency;
using namespace NJobAgent;

////////////////////////////////////////////////////////////////////

class TJobProbeService
	: public TServiceBase
{
public:
	TJobProbeService(TJobProxy* jobProxy)
		: TServiceBase(
			jobProxy->GetControlInvoker(),
            TJobProbeServiceProxy::GetServiceName(),
            JobProxyLogger,
            TJobProbeServiceProxy::GetProtocolVersion())
        , JobProxy_(jobProxy)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateInputContext));
    }

private:
	TJobProxy* JobProxy_;

    DECLARE_RPC_SERVICE_METHOD(NJobProbeClient::NProto, GenerateInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        std::vector<NChunkClient::TChunkId> inputContexts = JobProxy_->GenerateInputContext(jobId);
        context->SetResponseInfo("ChunkId: %v", JoinToString(inputContexts));

        for (const auto& inputContext : inputContexts) {
            ToProto(response->add_chunk_id(), inputContext);
        }
        context->Reply();
    }
};

IServicePtr CreateJobProbeService(TJobProxy* jobProxy)
{
	return New<TJobProbeService>(jobProxy);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT