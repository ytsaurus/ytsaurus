#include "client_impl.h"

#include <yt/ytlib/node_tracker_client/channel.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYTree;
using namespace NJobTrackerClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TYsonString TClient::DoStraceJob(
    TJobId jobId,
    const TStraceJobOptions& /*options*/)
{
    auto req = JobProberProxy_->Strace();
    ToProto(req->mutable_job_id(), jobId);

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    return TYsonString(rsp->trace());
}

void TClient::DoSignalJob(
    TJobId jobId,
    const TString& signalName,
    const TSignalJobOptions& /*options*/)
{
    auto req = JobProberProxy_->SignalJob();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_signal_name(), signalName);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

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
    auto jobNodeDescriptor = GetJobNodeDescriptor(jobId, EPermissionSet(EPermission::Manage | EPermission::Read))
        .ValueOrThrow();
    auto nodeChannel = ChannelFactory_->CreateChannel(jobNodeDescriptor);

    YT_LOG_DEBUG("Polling job shell (JobId: %v)", jobId);

    NJobProberClient::TJobProberServiceProxy proxy(nodeChannel);

    auto spec = GetJobSpecFromJobNode(jobId, proxy)
        .ValueOrThrow();

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
