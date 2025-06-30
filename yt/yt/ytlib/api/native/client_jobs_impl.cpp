#include "client_impl.h"

#include "connection.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/ytlib/controller_agent/helpers.h>
#include <yt/yt/ytlib/controller_agent/job_prober_service_proxy.h>

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/client/api/transaction.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYTree;
using namespace NJobProberClient;
using namespace NJobTrackerClient;
using namespace NConcurrency;

using NScheduler::AllocationIdFromJobId;

////////////////////////////////////////////////////////////////////////////////

namespace {

void RequestJobInterruption(
    const NControllerAgent::TJobProberServiceProxy& jobProberProxy,
    TJobId jobId,
    TOperationId operationId,
    NControllerAgent::TIncarnationId agentIncarnarionId,
    TDuration timeout)
{
    auto req = jobProberProxy.InterruptJob();

    ToProto(req->mutable_incarnation_id(), agentIncarnarionId);

    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_operation_id(), operationId);

    req->set_timeout(ToProto(timeout));

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        if (IsRevivalError(rspOrError)) {
            THROW_ERROR_EXCEPTION("Failed to interrupt job")
                << MakeRevivalError(operationId, jobId);
        }

        THROW_ERROR_EXCEPTION(
            "Error interrupting job %v of operation %v",
            jobId,
            operationId);
    }
}

void RequestJobAbort(
    const TJobProberServiceProxy& jobProberProxy,
    TJobId jobId,
    const std::string& user)
{
    auto error = TError("Job aborted by user request")
        << TErrorAttribute("abort_reason", NScheduler::EAbortReason::UserRequest)
        << TErrorAttribute("user", user);

    auto req = jobProberProxy.Abort();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_error(), error);

    auto rspOrError = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        rspOrError,
        "Error aborting job %v",
        jobId);
}

} // namespace

void TClient::DoAbandonJob(
    TJobId jobId,
    const TAbandonJobOptions& /*options*/)
{
    auto allocationId = AllocationIdFromJobId(jobId);

    auto allocationBriefInfo = WaitFor(GetAllocationBriefInfo(
        *SchedulerOperationProxy_,
        allocationId,
        NScheduler::TAllocationInfoToRequest{
            .OperationId = true,
            .OperationAcl = true,
            .OperationAcoName = true,
            .ControllerAgentDescriptor = true,
        }))
        .ValueOrThrow();

    ValidateOperationAccess(
        allocationBriefInfo.OperationId,
        jobId,
        GetAcrFromAllocationBriefInfo(allocationBriefInfo),
        EPermissionSet(EPermission::Manage));

    NControllerAgent::TJobProberServiceProxy jobProberProxy(
        ChannelFactory_->CreateChannel(
            *allocationBriefInfo.ControllerAgentDescriptor.Addresses));
    // TODO(nadya02): Set the correct timeout here.
    jobProberProxy.SetDefaultTimeout(NRpc::DefaultRpcRequestTimeout);

    auto request = jobProberProxy.AbandonJob();
    ToProto(request->mutable_incarnation_id(), allocationBriefInfo.ControllerAgentDescriptor.IncarnationId);
    ToProto(request->mutable_operation_id(), allocationBriefInfo.OperationId);
    ToProto(request->mutable_job_id(), jobId);

    auto error = WaitFor(request->Invoke());
    if (!error.IsOK()) {
        if (IsRevivalError(error)) {
            THROW_ERROR_EXCEPTION("Failed to abandon job")
                << MakeRevivalError(allocationBriefInfo.OperationId, jobId);
        }
        THROW_ERROR(error);
    }
}

TPollJobShellResponse TClient::DoPollJobShell(
    TJobId jobId,
    const std::optional<TString>& shellName,
    const TYsonString& parameters,
    const TPollJobShellOptions& /*options*/)
{
    YT_LOG_DEBUG(
        "Polling job shell (JobId: %v, ShellName: %v)",
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

    YT_LOG_DEBUG(
        "Received job shell descriptor (JobShellDescriptor: %v)",
        jobShellDescriptor);

    auto nodeChannel = ChannelFactory_->CreateChannel(jobShellDescriptor.NodeDescriptor);
    auto proxy = CreateNodeJobProberServiceProxy(std::move(nodeChannel));

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

    return TPollJobShellResponse{
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
    auto allocationId = AllocationIdFromJobId(jobId);

    auto allocationBriefInfo = WaitFor(GetAllocationBriefInfo(
        *SchedulerOperationProxy_,
        allocationId,
        NScheduler::TAllocationInfoToRequest{
            .OperationId = true,
            .OperationAcl = true,
            .OperationAcoName = true,
            .ControllerAgentDescriptor = true,
            .NodeDescriptor = true,
        }))
        .ValueOrThrow();

    ValidateOperationAccess(
        allocationBriefInfo.OperationId,
        jobId,
        GetAcrFromAllocationBriefInfo(allocationBriefInfo),
        EPermissionSet(EPermission::Manage));

    if (options.InterruptTimeout.value_or(TDuration::Zero()) != TDuration::Zero()) {
        NControllerAgent::TJobProberServiceProxy proxy(ChannelFactory_->CreateChannel(
            *allocationBriefInfo.ControllerAgentDescriptor.Addresses));
        // TODO(nadya02): Set the correct timeout here.
        proxy.SetDefaultTimeout(NRpc::DefaultRpcRequestTimeout);
        RequestJobInterruption(
            proxy,
            jobId,
            allocationBriefInfo.OperationId,
            allocationBriefInfo.ControllerAgentDescriptor.IncarnationId,
            *options.InterruptTimeout);
    } else {
        auto proxy = CreateNodeJobProberServiceProxy(
            ChannelFactory_->CreateChannel(allocationBriefInfo.NodeDescriptor));
        RequestJobAbort(proxy, jobId, Options_.GetAuthenticatedUser());
    }
}

void TClient::DoDumpJobProxyLog(
    TJobId jobId,
    TOperationId operationId,
    const TYPath& path,
    const TDumpJobProxyLogOptions& /*options*/)
{
    ValidateOperationAccess(operationId, jobId, EPermissionSet(EPermission::Read));

    auto nodeChannel = TryCreateChannelToJobNode(operationId, jobId, EPermissionSet(EPermission::Read))
        .ValueOrThrow();

    auto jobProberServiceProxy = CreateNodeJobProberServiceProxy(std::move(nodeChannel));

    auto transaction = [&] {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Dump job proxy logs of job %v of operation %v", jobId, operationId));

        NApi::TTransactionStartOptions options{
            .Attributes = std::move(attributes)
        };

        return WaitFor(StartTransaction(NTransactionClient::ETransactionType::Master, options))
            .ValueOrThrow();
    }();

    auto req = jobProberServiceProxy.DumpJobProxyLog();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_path(), path);
    ToProto(req->mutable_transaction_id(), transaction->GetId());

    WaitFor(req->Invoke())
        .ThrowOnError();

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
