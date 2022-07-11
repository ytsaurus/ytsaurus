#include "client_impl.h"
#include "connection.h"
#include "config.h"

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYTree;
using namespace NJobProberClient;
using namespace NJobTrackerClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TClient::DoAbandonJob(
    TJobId jobId,
    const TAbandonJobOptions& /*options*/)
{
    auto req = SchedulerJobProberProxy_->AbandonJob();
    ToProto(req->mutable_job_id(), jobId);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

TPollJobShellResponse TClient::DoPollJobShell(
    TJobId jobId,
    const std::optional<TString>& shellName,
    const TYsonString& parameters,
    const TPollJobShellOptions& /*options*/)
{
    YT_LOG_DEBUG("Polling job shell (JobId: %v, ShellName: %v)",
        jobId,
        shellName);

    const auto& jobShellDescriptorCache = Connection_->GetJobShellDescriptorCache();
    TJobShellDescriptorKey jobShellDescriptorKey{
        .User = Options_.GetAuthenticatedUser(),
        .JobId = jobId,
        .ShellName = shellName
    };

    auto jobShellDescriptor = WaitFor(jobShellDescriptorCache->Get(jobShellDescriptorKey))
        .ValueOrThrow();

    YT_LOG_DEBUG("Received job shell descriptor (JobShellDescriptor: %v)",
        jobShellDescriptor);

    auto nodeChannel = ChannelFactory_->CreateChannel(jobShellDescriptor.NodeDescriptor);
    TJobProberServiceProxy proxy(std::move(nodeChannel));
    proxy.SetDefaultTimeout(Connection_->GetConfig()->JobProberRpcTimeout);

    auto req = proxy.PollJobShell();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_parameters(), parameters.ToString());
    req->set_subcontainer(jobShellDescriptor.Subcontainer);

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error polling job shell")
            << TErrorAttribute("job_id", jobId)
            << TErrorAttribute("shell_name", shellName)
            << TErrorAttribute("subcontainer", jobShellDescriptor.Subcontainer)
            << rspOrError;
    }

    const auto& rsp = rspOrError.Value();

    return TPollJobShellResponse {
        .Result = TYsonString(rsp->result()),
        .LoggingContext = rsp->has_logging_context() 
            ? TYsonString(rsp->logging_context(), NYson::EYsonType::MapFragment)
            : TYsonString(),
    };
}

void TClient::DoAbortJob(
    TJobId jobId,
    const TAbortJobOptions& options)
{
    auto req = SchedulerJobProberProxy_->AbortJob();
    ToProto(req->mutable_job_id(), jobId);
    if (options.InterruptTimeout) {
        req->set_interrupt_timeout(ToProto<i64>(*options.InterruptTimeout));
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
