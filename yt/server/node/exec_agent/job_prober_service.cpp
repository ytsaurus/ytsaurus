#include "job_prober_service.h"
#include "private.h"

#include <yt/server/node/cluster_node/bootstrap.h>

#include <yt/server/node/job_agent/job_controller.h>

#include <yt/ytlib/job_prober_client/job_prober_service_proxy.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NExecAgent {

using namespace NRpc;
using namespace NJobProberClient;
using namespace NConcurrency;
using namespace NJobAgent;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TJobProberService
    : public TServiceBase
{
public:
    explicit TJobProberService(NClusterNode::TBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetJobInvoker(),
            TJobProberServiceProxy::GetDescriptor(),
            ExecAgentLogger)
        , Bootstrap_(bootstrap)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DumpInputContext));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStderr));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Strace));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SignalJob));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PollJobShell)
            .SetInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Interrupt));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Abort));

        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
    }

private:
    NClusterNode::TBootstrap* const Bootstrap_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);


    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, DumpInputContext)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        auto chunkIds = job->DumpInputContext();

        context->SetResponseInfo("ChunkIds: %v", chunkIds);
        ToProto(response->mutable_chunk_ids(), chunkIds);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GetStderr)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->FindRecentlyRemovedJob(jobId);
        if (!job) {
            job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        }

        auto stderrData = job->GetStderr();

        response->set_stderr_data(stderrData);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, GetSpec)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->FindRecentlyRemovedJob(jobId);
        if (!job) {
            job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        }
        response->mutable_spec()->CopyFrom(job->GetSpec());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Strace)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        auto trace = job->StraceJob();

        ToProto(response->mutable_trace(), trace.GetData());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, SignalJob)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        const auto& signalName = request->signal_name();
        context->SetRequestInfo("JobId: %v, SignalName: %v",
            jobId,
            signalName);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        job->SignalJob(signalName);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, PollJobShell)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto jobId = FromProto<TJobId>(request->job_id());
        auto parameters = TYsonString(request->parameters());

        context->SetRequestInfo("JobId: %v",
            jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        auto result = job->PollJobShell(parameters);

        response->set_result(result.GetData());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Interrupt)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());

        context->SetRequestInfo("JobId: %v",
            jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        job->Interrupt();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProberClient::NProto, Abort)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobId = FromProto<TJobId>(request->job_id());
        auto error = FromProto<TError>(request->error());

        context->SetRequestInfo("JobId: %v", jobId);

        auto job = Bootstrap_->GetJobController()->GetJobOrThrow(jobId);
        job->Abort(error);

        if (job->GetPhase() < EJobPhase::WaitingAbort) {
            THROW_ERROR_EXCEPTION("Failed to abort job %v", jobId)
                << TErrorAttribute("job_state", job->GetState())
                << TErrorAttribute("job_phase", job->GetPhase());
        }

        context->Reply();
    }
};

IServicePtr CreateJobProberService(NClusterNode::TBootstrap* bootstrap)
{
    return New<TJobProberService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecAgent
