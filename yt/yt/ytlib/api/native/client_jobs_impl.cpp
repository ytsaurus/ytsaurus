#include "client_impl.h"

#include "connection.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/ytlib/controller_agent/helpers.h>
#include <yt/yt/ytlib/controller_agent/job_prober_service_proxy.h>

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/shell/public.h>

#include <yt/yt/client/api/transaction.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYTree;
using namespace NJobProberClient;
using namespace NJobTrackerClient;
using namespace NConcurrency;

using NScheduler::AllocationIdFromJobId;

////////////////////////////////////////////////////////////////////////////////

class TJobShellCommandOutputReader
    : public IAsyncZeroCopyInputStream
{
public:
    TJobShellCommandOutputReader(
        IClientPtr client,
        TJobId jobId,
        std::string shellId,
        std::optional<TString> shellName,
        const NLogging::TLogger& logger)
        : Client_(std::move(client))
        , JobId_(jobId)
        , ShellName_(std::move(shellName))
        , ShellId_(std::move(shellId))
        , Logger(logger)
    { }

    TFuture<TSharedRef> Read() override
    {
        auto parameters = BuildYsonStringFluently()
            .BeginMap()
                .Item("operation").Value("poll")
                .Item("shell_id").Value(ShellId_)
            .EndMap();

        return Client_->PollJobShell(JobId_, ShellName_, parameters, {}).Apply(BIND(
            [this, this_ = MakeStrong(this)] (const TErrorOr<TPollJobShellResponse>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    auto error = TError(rspOrError);

                    if (error.FindMatching(NShell::EErrorCode::ShellExited) ||
                        error.FindMatching(NShell::EErrorCode::ShellManagerShutDown))
                    {
                        YT_LOG_DEBUG(error, "Job shell exited (JobId: %v, ShellId: %v)", JobId_, ShellId_);
                        return TSharedRef();
                    }

                    THROW_ERROR_EXCEPTION("Error polling job shell")
                        << TErrorAttribute("job_id", JobId_)
                        << TErrorAttribute("shell_name", ShellName_)
                        << TErrorAttribute("shell_id", ShellId_)
                        << error;
                }

                const auto& rsp = rspOrError.Value();
                auto result = ConvertToNode(rsp.Result);
                auto resultMap = result->AsMap();

                auto output = resultMap->FindChildValue<std::string>("output");

                if (!output) {
                    THROW_ERROR_EXCEPTION("No output from job shell")
                        << TErrorAttribute("job_id", JobId_)
                        << TErrorAttribute("shell_name", ShellName_)
                        << TErrorAttribute("shell_id", ShellId_);
                }

                return TSharedRef::FromString(*output);
            }));
    }

private:
    const IClientPtr Client_;
    const TJobId JobId_;
    const std::optional<TString> ShellName_;
    const std::string ShellId_;

    const NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TJobShellCommandOutputReader)

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
    const TAbandonJobOptions& options)
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
    jobProberProxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultAbandonJobTimeout));

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
        .ShellName = shellName,
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

IAsyncZeroCopyInputStreamPtr TClient::DoRunJobShellCommand(
    TJobId jobId,
    const std::optional<std::string>& shellName,
    const std::string& command,
    const TRunJobShellCommandOptions& /*options*/)
{
    // TODO(bystrovserg): Just remove it after TJobShellDescriptorKey migrates to std::string.
    auto shellNameConverted = shellName ? std::optional<TString>(*shellName) : std::nullopt;

    auto spawnParameters = BuildYsonStringFluently()
        .BeginMap()
            .Item("operation").Value("spawn")
            .Item("command").Value(command)
        .EndMap();

    auto rsp = WaitFor(PollJobShell(jobId, shellNameConverted, spawnParameters, {}))
        .ValueOrThrow();

    auto result = ConvertToNode(rsp.Result);
    auto shellId = result->AsMap()->GetChildValueOrThrow<std::string>("shell_id");

    YT_LOG_DEBUG("Job shell spawned (JobId: %v, ShellId: %v)",
        jobId,
        shellId);

    return New<TJobShellCommandOutputReader>(
        StaticPointerCast<IClient>(MakeStrong(this)),
        jobId,
        shellId,
        std::move(shellNameConverted),
        Logger);
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
        proxy.SetDefaultTimeout(options.Timeout.value_or(Connection_->GetConfig()->DefaultAbortJobTimeout));
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
