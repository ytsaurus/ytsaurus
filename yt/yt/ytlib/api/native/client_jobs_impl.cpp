#include "client_impl.h"

#include <yt/ytlib/node_tracker_client/channel.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYTree;
using namespace NJobTrackerClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TClient::DoAbandonJob(
    TJobId jobId,
    const TAbandonJobOptions& /*options*/)
{
    auto req = JobProberProxy_->AbandonJob();
    ToProto(req->mutable_job_id(), jobId);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

TYsonString TClient::DoPollJobShell(
    TJobId jobId,
    const TYsonString& parameters,
    const TPollJobShellOptions& options)
{
    auto jobNodeDescriptor = TryGetJobNodeDescriptor(jobId, EPermissionSet(EPermission::Manage | EPermission::Read))
        .ValueOrThrow();
    auto nodeChannel = ChannelFactory_->CreateChannel(jobNodeDescriptor);
    NJobProberClient::TJobProberServiceProxy proxy(nodeChannel);

    YT_LOG_DEBUG("Polling job shell (JobId: %v)", jobId);
    auto req = proxy.PollJobShell();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_parameters(), parameters.GetData());

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error polling job shell")
            << TErrorAttribute("job_id", jobId)
            << rspOrError;
    }

    const auto& rsp = rspOrError.Value();
    return TYsonString(rsp->result());
}

void TClient::DoAbortJob(
    TJobId jobId,
    const TAbortJobOptions& options)
{
    auto req = JobProberProxy_->AbortJob();
    ToProto(req->mutable_job_id(), jobId);
    if (options.InterruptTimeout) {
        req->set_interrupt_timeout(ToProto<i64>(*options.InterruptTimeout));
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
