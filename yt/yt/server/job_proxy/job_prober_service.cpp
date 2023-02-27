#include "job_prober_service.h"

#include <yt/yt/ytlib/job_prober_client/job_probe.h>
#include <yt/yt/ytlib/job_prober_client/job_prober_service_proxy.h>
#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/server/tools/tools.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/fs.h>

#include <util/system/fs.h>

namespace NYT::NJobProber {

using namespace NConcurrency;
using namespace NJobProberClient;
using namespace NRpc;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger JobProberLogger("JobProber");

////////////////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    TJobProberService(IJobProbePtr jobProxy, IInvokerPtr controlInvoker)
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpSensors));
    }

private:
    TWeakPtr<IJobProbe> JobProxy_;

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        auto chunkIds = GetJobProxy()->DumpInputContext();
        context->SetResponseInfo("ChunkIds: %v", chunkIds);

        ToProto(response->mutable_chunk_ids(), chunkIds);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GetStderr)
    {
        auto stderrData = GetJobProxy()->GetStderr();

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

        auto pollJobShellResponse = GetJobProxy()->PollJobShell(jobShellDescriptor, parameters);
        response->set_result(pollJobShellResponse.Result.ToString());
        if (pollJobShellResponse.LoggingContext) {
            response->set_logging_context(pollJobShellResponse.LoggingContext.ToString());
            context->SetResponseInfo(
                "LoggingContext: %v",
                pollJobShellResponse.LoggingContext);
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Interrupt)
    {
        Y_UNUSED(response);

        GetJobProxy()->Interrupt();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Fail)
    {
        Y_UNUSED(response);

        GetJobProxy()->Fail();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpSensors)
    {
        response->Attachments().push_back(GetJobProxy()->DumpSensors());

        context->Reply();
    }

    IJobProbePtr GetJobProxy()
    {
        auto jobProxy = JobProxy_.Lock();
        if (!jobProxy) {
            THROW_ERROR_EXCEPTION("Job is missing");
        }
        return jobProxy;
    }
};

IServicePtr CreateJobProberService(IJobProbePtr jobProbe, IInvokerPtr controlInvoker)
{
    return New<TJobProberService>(jobProbe, controlInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProber
