#include "controller_agent_connector.h"

#include "bootstrap.h"
#include "helpers.h"
#include "job.h"
#include "job_controller.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/controller_agent/job_spec_service_proxy.h>
#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NNodeTrackerClient;

using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NScheduler::NProto::NNode;

using NScheduler::TIncarnationId;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TControllerAgentConnectorPool::TControllerAgentConnector::TControllerAgentConnector(
    TControllerAgentConnectorPool* controllerAgentConnectorPool,
    TControllerAgentDescriptor controllerAgentDescriptor)
    : ControllerAgentConnectorPool_(MakeStrong(controllerAgentConnectorPool))
    , ControllerAgentDescriptor_(std::move(controllerAgentDescriptor))
    , Channel_(ControllerAgentConnectorPool_->CreateChannel(ControllerAgentDescriptor_))
    , HeartbeatExecutor_(New<TPeriodicExecutor>(
        ControllerAgentConnectorPool_->Bootstrap_->GetControlInvoker(),
        BIND_NO_PROPAGATE(&TControllerAgentConnector::SendHeartbeat, MakeWeak(this)),
        TPeriodicExecutorOptions{
            .Period = GetConfig()->HeartbeatPeriod,
            .Splay = GetConfig()->HeartbeatSplay
        }))
    , StatisticsThrottler_(CreateReconfigurableThroughputThrottler(
        GetConfig()->StatisticsThrottler))
{
    YT_LOG_DEBUG("Controller agent connector created (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);
    HeartbeatExecutor_->Start();
}

NRpc::IChannelPtr TControllerAgentConnectorPool::TControllerAgentConnector::GetChannel() const noexcept
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Channel_;
}

void TControllerAgentConnectorPool::TControllerAgentConnector::SendOutOfBandHeartbeatIfNeeded()
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    if (ShouldSendOutOfBand_) {
        HeartbeatExecutor_->ScheduleOutOfBand();
        ShouldSendOutOfBand_ = false;
    }
}

void TControllerAgentConnectorPool::TControllerAgentConnector::EnqueueFinishedJob(const TJobPtr& job)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    EnqueuedFinishedJobs_.insert(job);
    ShouldSendOutOfBand_ = true;
}

const TControllerAgentDescriptor& TControllerAgentConnectorPool::TControllerAgentConnector::GetDescriptor() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ControllerAgentDescriptor_;
}

void TControllerAgentConnectorPool::TControllerAgentConnector::AddUnconfirmedJobIds(std::vector<TJobId> unconfirmedJobIds)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    UnconfirmedJobIds_ = std::move(unconfirmedJobIds);
}

std::vector<TFuture<TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo>>
TControllerAgentConnectorPool::TControllerAgentConnector::SettleJobsViaJobSpecService(
    const std::vector<TAllocationInfo>& allocationInfos)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    TJobSpecServiceProxy jobSpecServiceProxy(Channel_);

    auto settleJobsTimeout = GetConfig()->SettleJobsTimeout;

    jobSpecServiceProxy.SetDefaultTimeout(settleJobsTimeout);
    auto settleJobRequest = jobSpecServiceProxy.GetJobSpecs();

    for (auto [allocationId, operationId] : allocationInfos) {
        auto* subrequest = settleJobRequest->add_requests();
        ToProto(subrequest->mutable_operation_id(), operationId);
        ToProto(subrequest->mutable_allocation_id(), allocationId);

        YT_LOG_DEBUG(
            "Add settle job spec request (AgentDescriptor: %v, AllocationId: %v, OperationId: %v)",
            ControllerAgentDescriptor_,
            allocationId,
            operationId);

        EmplaceOrCrash(AllocationIdsWaitingForSpec_, allocationId, operationId);
    }

    YT_LOG_DEBUG(
        "Requesting job specs (AgentDescriptor: %v, Count: %v)",
        ControllerAgentDescriptor_,
        std::size(allocationInfos));

    auto settleJobsFuture = settleJobRequest->Invoke();

    std::vector<TFuture<TJobStartInfo>> result(std::size(allocationInfos));

    for (int index = 0; index < std::ssize(allocationInfos); ++index) {
        result[index] = settleJobsFuture.Apply(BIND([
            this,
            this_ = MakeStrong(this),
            allocationIndex = index,
            allocationCount = std::ssize(allocationInfos),
            allocationInfo = allocationInfos[index]
        ] (const TErrorOr<TIntrusivePtr<TTypedClientResponse<TRspGetJobSpecs>>>& rspOrError) -> TJobStartInfo {
            if (!rspOrError.IsOK()) {
                YT_LOG_DEBUG(
                    rspOrError,
                    "Failed to settle job (ControllerAgentDescriptor: %v, AllocationId: %v, OperationId: %v)",
                    ControllerAgentDescriptor_,
                    allocationInfo.AllocationId,
                    allocationInfo.OperationId);

                THROW_ERROR rspOrError;
            }

            const auto& rsp = rspOrError.Value();

            YT_LOG_DEBUG(
                "Settle job response received (ControllerAgentDescriptor: %v, AllocationId: %v, OperationId: %v)",
                ControllerAgentDescriptor_,
                allocationInfo.AllocationId,
                allocationInfo.OperationId);

            YT_VERIFY(rsp->responses_size() == allocationCount);
            YT_VERIFY(std::ssize(rsp->Attachments()) == allocationCount);

            const auto& subresponse = rsp->mutable_responses(allocationIndex);
            if (auto error = FromProto<TError>(subresponse->error()); !error.IsOK()) {
                YT_LOG_DEBUG(
                    error,
                    "No job is available for allocation (ControllerAgentDescriptor: %v, OperationId: %v, AllocationId: %v)",
                    ControllerAgentDescriptor_,
                    allocationInfo.OperationId,
                    allocationInfo.AllocationId);

                THROW_ERROR error;
            }

            auto jobId = FromProto<TJobId>(subresponse->job_id());

            TJobSpec spec;
            DeserializeProtoWithEnvelope(&spec, rsp->Attachments()[allocationIndex]);

            auto result = TJobStartInfo{
                .JobId = jobId,
                .JobSpec = std::move(spec),
            };

            return result;
        })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    return result;
}

std::vector<TFuture<TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo>>
TControllerAgentConnectorPool::TControllerAgentConnector::SettleJobsViaJobTrackerService(
    const std::vector<TAllocationInfo>& allocationInfos)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    TJobTrackerServiceProxy jobTrackerServiceProxy(Channel_);

    auto settleJobsTimeout = GetConfig()->SettleJobsTimeout;

    jobTrackerServiceProxy.SetDefaultTimeout(settleJobsTimeout);

    std::vector<TFuture<TJobStartInfo>> result(std::size(allocationInfos));

    for (int index = 0; index < std::ssize(allocationInfos); ++index) {
        const auto& allocationInfo = allocationInfos[index];

        YT_LOG_DEBUG(
            "Add settle job spec request (AgentDescriptor: %v, AllocationId: %v, OperationId: %v)",
            ControllerAgentDescriptor_,
            allocationInfo.AllocationId,
            allocationInfo.OperationId);

        EmplaceOrCrash(
            AllocationIdsWaitingForSpec_,
            allocationInfo.AllocationId,
            allocationInfo.OperationId);

        auto settleJobRequest = jobTrackerServiceProxy.SettleJob();

        const auto* bootstrap = ControllerAgentConnectorPool_->Bootstrap_;

        SetNodeInfoToRequest(
            bootstrap->GetNodeId(),
            bootstrap->GetLocalDescriptor(),
            settleJobRequest);

        ToProto(settleJobRequest->mutable_controller_agent_incarnation_id(), ControllerAgentDescriptor_.IncarnationId);

        ToProto(settleJobRequest->mutable_allocation_id(), allocationInfo.AllocationId);
        ToProto(settleJobRequest->mutable_operation_id(), allocationInfo.OperationId);

        result[index] = settleJobRequest->Invoke().Apply(BIND([
                this,
                this_ = MakeStrong(this),
                allocationInfo
            ] (const TErrorOr<TIntrusivePtr<TTypedClientResponse<TRspSettleJob>>>& rspOrError) -> TJobStartInfo {
                if (!rspOrError.IsOK()) {
                    YT_LOG_DEBUG(
                        rspOrError,
                        "Failed to settle job (ControllerAgentDescriptor: %v, AllocationId: %v, OperationId: %v)",
                        ControllerAgentDescriptor_,
                        allocationInfo.AllocationId,
                        allocationInfo.OperationId);

                    THROW_ERROR rspOrError;
                }

                const auto& rsp = rspOrError.Value();

                YT_LOG_DEBUG(
                    "Settle job response received (ControllerAgentDescriptor: %v, AllocationId: %v, OperationId: %v)",
                    ControllerAgentDescriptor_,
                    allocationInfo.AllocationId,
                    allocationInfo.OperationId);

                switch (rsp->job_info_or_error_case()) {
                    case TRspSettleJob::JobInfoOrErrorCase::kError: {
                        auto error = FromProto<TError>(rsp->error());

                        YT_LOG_DEBUG(
                            error,
                            "No job is available for allocation (ControllerAgentDescriptor: %v, OperationId: %v, AllocationId: %v)",
                            ControllerAgentDescriptor_,
                            allocationInfo.OperationId,
                            allocationInfo.AllocationId);

                        YT_VERIFY(!error.IsOK());

                        THROW_ERROR std::move(error);
                    }
                    case TRspSettleJob::JobInfoOrErrorCase::kJobInfo: {
                        auto jobId = FromProto<TJobId>(rsp->job_info().job_id());

                        YT_VERIFY(std::size(rsp->Attachments()) == 1);

                        TJobSpec spec;
                        DeserializeProtoWithEnvelope(&spec, rsp->Attachments()[0]);

                        return TJobStartInfo{
                            .JobId = jobId,
                            .JobSpec = std::move(spec),
                        };
                    }

                    default:
                        YT_LOG_FATAL(
                            "Unexpected value in job_info_or_error (ControllerAgentDescriptor: %v, OperationId: %v, AllocationId: %v, Value: %v",
                            ControllerAgentDescriptor_,
                            allocationInfo.OperationId,
                            allocationInfo.AllocationId,
                            static_cast<int>(rsp->job_info_or_error_case()));
                }
            })
            .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    return result;
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnConfigUpdated(
    const TControllerAgentConnectorDynamicConfigPtr& newConfig)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    YT_LOG_DEBUG(
        "Set new controller agent heartbeat period (NewPeriod: %v)",
        newConfig->HeartbeatPeriod);

    HeartbeatExecutor_->SetPeriod(newConfig->HeartbeatPeriod);
    StatisticsThrottler_->Reconfigure(newConfig->StatisticsThrottler);
}

TControllerAgentConnectorPool::TControllerAgentConnector::~TControllerAgentConnector()
{
    YT_LOG_DEBUG("Controller agent connector destroyed (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);

    YT_UNUSED_FUTURE(HeartbeatExecutor_->Stop());
}

TControllerAgentConnectorDynamicConfigPtr
TControllerAgentConnectorPool::TControllerAgentConnector::GetConfig() const noexcept
{
    return ControllerAgentConnectorPool_->DynamicConfig_.Acquire();
}

void TControllerAgentConnectorPool::TControllerAgentConnector::DoSendHeartbeat()
{
    const auto* bootstrap = ControllerAgentConnectorPool_->Bootstrap_;

    VERIFY_INVOKER_AFFINITY(bootstrap->GetControlInvoker());

    auto nodeId = bootstrap->GetNodeId();
    auto nodeDescriptor = bootstrap->GetLocalDescriptor();

    if (!bootstrap->IsConnected() || nodeId == InvalidNodeId) {
        return;
    }

    if (TInstant::Now() < HeartbeatInfo_.LastFailedHeartbeatTime + HeartbeatInfo_.FailedHeartbeatBackoffTime) {
        YT_LOG_INFO(
            "Skipping heartbeat to agent since backoff after previous heartbeat failure (AgentAddress: %v, IncarnationId: %v)",
            ControllerAgentDescriptor_.Address,
            ControllerAgentDescriptor_.IncarnationId);
        return;
    }

    TJobTrackerServiceProxy proxy(Channel_);
    auto request = proxy.Heartbeat();

    auto context = New<TAgentHeartbeatContext>();
    auto currentConfig = GetConfig();

    if (currentConfig->TestHeartbeatDelay) {
        TDelayedExecutor::WaitForDuration(currentConfig->TestHeartbeatDelay);
    }

    PrepareHeartbeatRequest(nodeId, nodeDescriptor, request, context);

    HeartbeatInfo_.LastSentHeartbeatTime = TInstant::Now();

    auto requestFuture = request->Invoke();
    YT_LOG_INFO(
        "Heartbeat sent to agent (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);

    auto responseOrError = WaitFor(std::move(requestFuture));
    if (!responseOrError.IsOK()) {
        HeartbeatInfo_.LastFailedHeartbeatTime = TInstant::Now();

        auto heartbeatBackoffStartTime = currentConfig->FailedHeartbeatBackoffStartTime;
        auto heartbeatBackoffMaxTime = currentConfig->FailedHeartbeatBackoffMaxTime;
        auto heartbeatBackoffMultiplier = currentConfig->FailedHeartbeatBackoffMultiplier;

        if (HeartbeatInfo_.FailedHeartbeatBackoffTime == TDuration::Zero()) {
            HeartbeatInfo_.FailedHeartbeatBackoffTime = heartbeatBackoffStartTime;
        } else {
            HeartbeatInfo_.FailedHeartbeatBackoffTime = std::min(
                HeartbeatInfo_.FailedHeartbeatBackoffTime * heartbeatBackoffMultiplier,
                heartbeatBackoffMaxTime);
        }
        YT_LOG_ERROR(responseOrError, "Error reporting heartbeat to agent (AgentAddress: %v, BackoffTime: %v)",
            ControllerAgentDescriptor_.Address,
            HeartbeatInfo_.FailedHeartbeatBackoffTime);

        if (responseOrError.GetCode() == NControllerAgent::EErrorCode::IncarnationMismatch) {
            ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker()->Invoke(BIND(
                &TControllerAgentConnector::OnAgentIncarnationOutdated,
                MakeStrong(this)));
        }
        return;
    }

    auto& response = responseOrError.Value();
    ProcessHeartbeatResponse(response, context);

    YT_LOG_INFO(
        "Successfully reported heartbeat to agent (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);

    HeartbeatInfo_.FailedHeartbeatBackoffTime = TDuration::Zero();
}

// This method will be called in control thread when controller agent controls job lifetime.
void TControllerAgentConnectorPool::TControllerAgentConnector::SendHeartbeat()
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetControlInvoker());

    DoSendHeartbeat();
}

void TControllerAgentConnectorPool::TControllerAgentConnector::PrepareHeartbeatRequest(
    TNodeId nodeId,
    const TNodeDescriptor& nodeDescriptor,
    const TReqHeartbeatPtr& request,
    const TAgentHeartbeatContextPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto* bootstrap = ControllerAgentConnectorPool_->Bootstrap_;

    SetNodeInfoToRequest(
        nodeId,
        nodeDescriptor,
        request);

    auto error = WaitFor(BIND(
            &TControllerAgentConnector::DoPrepareHeartbeatRequest,
            MakeStrong(this),
            request,
            context)
        .AsyncVia(bootstrap->GetJobInvoker())
        .Run());

    YT_LOG_FATAL_IF(
        !error.IsOK(),
        error,
        "Failed to prepare agent heartbeat request");
}

void TControllerAgentConnectorPool::TControllerAgentConnector::ProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    const TAgentHeartbeatContextPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto error = WaitFor(BIND(
            &TControllerAgentConnector::DoProcessHeartbeatResponse,
            MakeStrong(this),
            response,
            context)
        .AsyncVia(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker())
        .Run());

    YT_LOG_FATAL_IF(
        !error.IsOK(),
        error,
        "Failed to process agent heartbeat response");
}

void TControllerAgentConnectorPool::TControllerAgentConnector::DoPrepareHeartbeatRequest(
    const TReqHeartbeatPtr& request,
    const TAgentHeartbeatContextPtr& context)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    context->ControllerAgentConnector = MakeStrong(this);
    context->StatisticsThrottler = StatisticsThrottler_;
    context->RunningJobStatisticsSendingBackoff = GetConfig()->RunningJobStatisticsSendingBackoff;
    context->NeedTotalConfirmation = IsTotalConfirmationNeeded();

    context->JobsToForcefullySend = EnqueuedFinishedJobs_;
    context->UnconfirmedJobIds = std::move(UnconfirmedJobIds_);
    context->AllocationIdsWaitingForSpec = AllocationIdsWaitingForSpec_;

    const auto& jobController = ControllerAgentConnectorPool_->Bootstrap_->GetJobController();
    jobController->PrepareAgentHeartbeatRequest(request, context);
}

void TControllerAgentConnectorPool::TControllerAgentConnector::DoProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    const TAgentHeartbeatContextPtr& context)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    const auto& jobController = ControllerAgentConnectorPool_->Bootstrap_->GetJobController();
    jobController->ProcessAgentHeartbeatResponse(response, context);

    for (auto finishedJobId : context->JobsToForcefullySend) {
        EnqueuedFinishedJobs_.erase(finishedJobId);
    }
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnAgentIncarnationOutdated() noexcept
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    YT_LOG_DEBUG(
        "Controller agent incarnation is outdated, stop connector (ControllerAgentDescriptor: %v)",
        ControllerAgentDescriptor_);

    const auto& jobController = ControllerAgentConnectorPool_->Bootstrap_->GetJobController();
    jobController->OnAgentIncarnationOutdated(ControllerAgentDescriptor_);

    EnqueuedFinishedJobs_.clear();

    YT_UNUSED_FUTURE(HeartbeatExecutor_->Stop());

    if (ControllerAgentConnectorPool_->ControllerAgentConnectors_.erase(ControllerAgentDescriptor_)) {
        YT_LOG_DEBUG(
            "Remove controller agent connector since incarnation is outdated (ControllerAgentDescriptor: %v)",
            ControllerAgentDescriptor_);
    } else {
        YT_LOG_DEBUG(
            "Controller agent connector is already removed (ControllerAgentDescriptor: %v)",
            ControllerAgentDescriptor_);
    }
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnJobRegistered(const TJobPtr& job)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    EraseOrCrash(AllocationIdsWaitingForSpec_, job->GetAllocationId());
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnJobRegistrationFailed(TAllocationId allocationId)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    EraseOrCrash(AllocationIdsWaitingForSpec_, allocationId);
}

bool TControllerAgentConnectorPool::TControllerAgentConnector::IsTotalConfirmationNeeded()
{
    auto now = TInstant::Now();

    if (now >= LastTotalConfirmationTime_ + TotalConfirmationPeriodMultiplicator_ * GetConfig()->TotalConfirmationPeriod) {
        LastTotalConfirmationTime_ = now;
        TotalConfirmationPeriodMultiplicator_ = GenerateTotalConfirmationPeriodMultiplicator();

        return true;
    }

    return false;
}

float TControllerAgentConnectorPool::TControllerAgentConnector::GenerateTotalConfirmationPeriodMultiplicator() noexcept
{
    return 0.9 + RandomNumber<float>() / 5;
}

////////////////////////////////////////////////////////////////////////////////

TControllerAgentConnectorPool::TControllerAgentConnectorPool(
    IBootstrap* const bootstrap)
    : DynamicConfig_(New<TControllerAgentConnectorDynamicConfig>())
    , Bootstrap_(bootstrap)
{ }

void TControllerAgentConnectorPool::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& jobController = Bootstrap_->GetJobController();

    jobController->SubscribeJobFinished(BIND_NO_PROPAGATE(
            &TControllerAgentConnectorPool::OnJobFinished,
            MakeWeak(this))
        .Via(Bootstrap_->GetJobInvoker()));

    jobController->SubscribeJobRegistered(BIND_NO_PROPAGATE(
            &TControllerAgentConnectorPool::OnJobRegistered,
            MakeWeak(this))
        .Via(Bootstrap_->GetJobInvoker()));

    jobController->SubscribeJobRegistrationFailed(BIND_NO_PROPAGATE(
            &TControllerAgentConnectorPool::OnJobRegistrationFailed,
            MakeWeak(this))
        .Via(Bootstrap_->GetJobInvoker()));
}

void TControllerAgentConnectorPool::SendOutOfBandHeartbeatsIfNeeded()
{
    for (auto& [agentDescriptor, controllerAgentConnector] : ControllerAgentConnectors_) {
        controllerAgentConnector->SendOutOfBandHeartbeatIfNeeded();
    }
}

TWeakPtr<TControllerAgentConnectorPool::TControllerAgentConnector>
TControllerAgentConnectorPool::GetControllerAgentConnector(
    const TJob* job)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    if (!job->GetControllerAgentDescriptor()) {
        return nullptr;
    }

    if (const auto it = ControllerAgentConnectors_.find(job->GetControllerAgentDescriptor());
        it != std::end(ControllerAgentConnectors_))
    {
        auto result = it->second;
        YT_VERIFY(result);
        return result;
    }

    YT_LOG_DEBUG(
        "Non-registered controller agent is assigned for job (JobId: %v, ControllerAgentDescriptor: %v)",
        job->GetId(),
        job->GetControllerAgentDescriptor());

    return nullptr;
}

void TControllerAgentConnectorPool::OnDynamicConfigChanged(
    const TControllerAgentConnectorDynamicConfigPtr& /*oldConfig*/,
    const TControllerAgentConnectorDynamicConfigPtr& newConfig)
{
    VERIFY_THREAD_AFFINITY_ANY();

    Bootstrap_->GetJobInvoker()->Invoke(
        BIND([
            this,
            this_ = MakeStrong(this),
            newConfig = std::move(newConfig)
        ] {
            OnConfigUpdated(newConfig);
            DynamicConfig_.Store(std::move(newConfig));
        }));
}

void TControllerAgentConnectorPool::OnRegisteredAgentSetReceived(
    THashSet<TControllerAgentDescriptor> controllerAgentDescriptors)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    YT_LOG_DEBUG(
        "Received registered controller agents (ControllerAgentCount: %v)",
        std::size(controllerAgentDescriptors));

    THashSet<TControllerAgentDescriptor> outdatedControllerAgentDescriptors;
    for (const auto& [descriptor, connector] : ControllerAgentConnectors_) {
        if (!controllerAgentDescriptors.contains(descriptor)) {
            YT_LOG_DEBUG(
                "Found outdated controller agent connector, remove it (ControllerAgentDescriptor: %v)",
                descriptor);

            EmplaceOrCrash(outdatedControllerAgentDescriptors, descriptor);
        } else {
            EraseOrCrash(controllerAgentDescriptors, descriptor);
        }
    }

    for (const auto& descriptor : outdatedControllerAgentDescriptors) {
        EraseOrCrash(ControllerAgentConnectors_, descriptor);
    }

    Bootstrap_->GetJobController()->OnControllerAgentIncarnationOutdated(outdatedControllerAgentDescriptors);

    for (auto& descriptor : controllerAgentDescriptors) {
        YT_LOG_DEBUG("Add new controller agent connector (ControllerAgentDescriptor: %v)", descriptor);
        AddControllerAgentConnector(std::move(descriptor));
    }
}

std::vector<TIncarnationId> TControllerAgentConnectorPool::GetRegisteredAgentIncarnationIds() const
{
    std::vector<TIncarnationId> incarnationIds;
    incarnationIds.reserve(ControllerAgentConnectors_.size());
    for (const auto& [descriptor, _] : ControllerAgentConnectors_) {
        incarnationIds.push_back(descriptor.IncarnationId);
    }
    return incarnationIds;
}

std::optional<TControllerAgentDescriptor> TControllerAgentConnectorPool::GetDescriptorByIncarnationId(TIncarnationId incarnationId) const
{
    for (const auto& [descriptor, _] : ControllerAgentConnectors_) {
        if (descriptor.IncarnationId == incarnationId) {
            return descriptor;
        }
    }
    return std::nullopt;
}

std::vector<TFuture<TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo>>
TControllerAgentConnectorPool::SettleJobs(
    const TControllerAgentDescriptor& agentDescriptor,
    const std::vector<TControllerAgentConnector::TAllocationInfo>& allocationInfos)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    auto controllerAgentConnector = GetControllerAgentConnector(agentDescriptor);

    if (DynamicConfig_.Acquire()->UseJobTrackerServiceToSettleJobs) {
        return controllerAgentConnector->SettleJobsViaJobTrackerService(allocationInfos);
    } else {
        return controllerAgentConnector->SettleJobsViaJobSpecService(allocationInfos);
    }
}

THashMap<TAllocationId, TOperationId> TControllerAgentConnectorPool::GetAllocationIdsWaitingForSpec() const
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    THashMap<TAllocationId, TOperationId> result;
    for (const auto& [agentDescriptor, agentConnector] : ControllerAgentConnectors_) {
        result.insert(
            std::begin(agentConnector->AllocationIdsWaitingForSpec_),
            std::end(agentConnector->AllocationIdsWaitingForSpec_));
    }

    return result;
}

IChannelPtr TControllerAgentConnectorPool::CreateChannel(const TControllerAgentDescriptor& agentDescriptor)
{
    const auto& client = Bootstrap_->GetClient();
    const auto& channelFactory = client->GetNativeConnection()->GetChannelFactory();
    return channelFactory->CreateChannel(agentDescriptor.Address);
}

IChannelPtr TControllerAgentConnectorPool::GetOrCreateChannel(
    const TControllerAgentDescriptor& agentDescriptor)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    if (const auto it = ControllerAgentConnectors_.find(agentDescriptor);
        it != std::end(ControllerAgentConnectors_))
    {
        return it->second->GetChannel();
    }

    return CreateChannel(agentDescriptor);
}

void TControllerAgentConnectorPool::OnConfigUpdated(
    const TControllerAgentConnectorDynamicConfigPtr& newConfig)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    for (const auto& [agentDescriptor, controllerAgentConnector] : ControllerAgentConnectors_) {
        controllerAgentConnector->OnConfigUpdated(newConfig);
    }
}

void TControllerAgentConnectorPool::OnJobFinished(const TJobPtr& job)
{
    if (auto controllerAgentConnector = job->GetControllerAgentConnector()) {
        controllerAgentConnector->EnqueueFinishedJob(job);

        YT_LOG_DEBUG(
            "Job finished, send out of band heartbeat to controller agent (JobId: %v, ControllerAgentDescriptor: %v)",
            job->GetId(),
            controllerAgentConnector->GetDescriptor());

        controllerAgentConnector->SendOutOfBandHeartbeatIfNeeded();
    }
}

TWeakPtr<TControllerAgentConnectorPool::TControllerAgentConnector>
TControllerAgentConnectorPool::AddControllerAgentConnector(
    TControllerAgentDescriptor agentDescriptor)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    auto controllerAgentConnector = New<TControllerAgentConnector>(this, agentDescriptor);

    EmplaceOrCrash(ControllerAgentConnectors_, std::move(agentDescriptor), controllerAgentConnector);

    return controllerAgentConnector;
}

TIntrusivePtr<TControllerAgentConnectorPool::TControllerAgentConnector>
TControllerAgentConnectorPool::GetControllerAgentConnector(
    const TControllerAgentDescriptor& agentDescriptor)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    return GetOrCrash(ControllerAgentConnectors_, agentDescriptor);
}

void TControllerAgentConnectorPool::OnJobRegistered(const TJobPtr& job)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    if (auto connector = job->GetControllerAgentConnector()) {
        connector->OnJobRegistered(job);
    }
}

void TControllerAgentConnectorPool::OnJobRegistrationFailed(
    TAllocationId allocationId,
    TOperationId /*operationId*/,
    const TControllerAgentDescriptor& agentDescriptor,
    const TError& /*error*/)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    if (!agentDescriptor) {
        return;
    }

    if (auto connectorIt = ControllerAgentConnectors_.find(agentDescriptor);
        connectorIt != std::end(ControllerAgentConnectors_))
    {
        connectorIt->second->OnJobRegistrationFailed(allocationId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
