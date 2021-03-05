#include "job_prober_service.h"

#include <yt/yt/ytlib/job_prober_client/job_probe.h>
#include <yt/yt/ytlib/job_prober_client/job_prober_service_proxy.h>
#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/ytlib/tools/tools.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/fs.h>

#include <util/system/fs.h>

namespace NYT::NJobProber {

using namespace NRpc;
using namespace NJobProberClient;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger JobProberLogger("JobProber");

////////////////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    TJobProberService(IJobProbe* jobProxy, IInvokerPtr controlInvoker)
        : TServiceBase(
            controlInvoker,
            TJobProberServiceProxy::GetDescriptor(),
            JobProberLogger)
        , JobProxy_(jobProxy)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStderr));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollJobShell));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Interrupt));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Fail));
    }

private:
    IJobProbe* JobProxy_;

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        auto chunkIds = JobProxy_->DumpInputContext();
        context->SetResponseInfo("ChunkIds: %v", chunkIds);

        ToProto(response->mutable_chunk_ids(), chunkIds);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GetStderr)
    {
        auto stderrData = JobProxy_->GetStderr();

        response->set_stderr_data(stderrData);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, PollJobShell)
    {
        auto parameters = TYsonString(request->parameters());

        TJobShellDescriptor jobShellDescriptor;
        jobShellDescriptor.Subcontainer = request->subcontainer();

        context->SetRequestInfo("Parameters: %v, Subcontainer: %v",
            ConvertToYsonString(parameters, EYsonFormat::Text),
            jobShellDescriptor.Subcontainer);

        auto result = JobProxy_->PollJobShell(jobShellDescriptor, parameters);
        response->set_result(result.ToString());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Interrupt)
    {
        Y_UNUSED(response);

        JobProxy_->Interrupt();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Fail)
    {
        Y_UNUSED(response);

        JobProxy_->Fail();

        context->Reply();
    }
};

IServicePtr CreateJobProberService(IJobProbe* jobProbe, IInvokerPtr controlInvoker)
{
    return New<TJobProberService>(jobProbe, controlInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProber
