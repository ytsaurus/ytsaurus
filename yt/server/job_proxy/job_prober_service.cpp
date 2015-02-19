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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
    }

private:
    TJobProxy* JobProxy_;

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v", jobId);

        auto chunkIds = JobProxy_->DumpInputContext(jobId);
        context->SetResponseInfo("ChunkId: [%v]", JoinToString(chunkIds));

        for (const auto& chunkId : chunkIds) {
            ToProto(response->add_chunk_id(), chunkId);
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
