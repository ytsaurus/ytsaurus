#include "stdafx.h"
#include "job_prober_service.h"
#include "private.h"

#include "job_proxy.h"

#include <ytlib/job_prober_client/job_prober_service_proxy.h>

#include <ytlib/chunk_client/public.h>

#include <core/rpc/service_detail.h>

namespace NYT {
namespace NJobProxy {

using namespace NRpc;
using namespace NJobProberClient;
using namespace NConcurrency;
using namespace NJobAgent;

////////////////////////////////////////////////////////////////////

class TJobProberService
	: public TServiceBase
{
public:
	TJobProberService(TJobProxy* jobProxy)
		: TServiceBase(
			jobProxy->GetControlInvoker(),
            TJobProberServiceProxy::GetServiceName(),
            JobProxyLogger,
            TJobProberServiceProxy::GetProtocolVersion())
        , JobProxy_(jobProxy)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateInputContext));
    }

private:
	TJobProxy* JobProxy_;

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GenerateInputContext)
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

IServicePtr CreateJobProberService(TJobProxy* jobProxy)
{
	return New<TJobProberService>(jobProxy);
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT