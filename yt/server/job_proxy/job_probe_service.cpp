#include "stdafx.h"
#include "job_probe_service.h"
#include "private.h"

#include "job_proxy.h"

#include <ytlib/job_probe_client/job_probe_service_proxy.h>

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

//        context->SetResponseInfo("ChunkId: %v", response->chunk_id())
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