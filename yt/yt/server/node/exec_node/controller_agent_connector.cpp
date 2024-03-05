#include "controller_agent_connector.h"

#include "bootstrap.h"
#include "helpers.h"
#include "job.h"
#include "job_controller.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/exec_node/config.h>
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
    , HeartbeatExecutor_(New<TRetryingPeriodicExecutor>(
        ControllerAgentConnectorPool_->Bootstrap_->GetControlInvoker(),
        BIND_NO_PROPAGATE([weakThis = MakeWeak(this)] {
            auto strongThis = weakThis.Lock();
            return strongThis ? strongThis->SendHeartbeat() : TError("Controller agent connector is destroyed");
        }),
        GetConfig()->HeartbeatExecutor))
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

TFuture<TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo>
TControllerAgentConnectorPool::TControllerAgentConnector::SettleJob(
    TOperationId operationId,
    TAllocationId allocationId)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    TJobTrackerServiceProxy jobTrackerServiceProxy(Channel_);

    auto settleJobsTimeout = GetConfig()->SettleJobsTimeout;

    jobTrackerServiceProxy.SetDefaultTimeout(settleJobsTimeout);

    YT_LOG_DEBUG(
        "Add settle job request (AgentDescriptor: %v, AllocationId: %v, OperationId: %v)",
        ControllerAgentDescriptor_,
        allocationId,
        operationId);

    EmplaceOrCrash(
        AllocationIdsWaitingForSpec_,
        allocationId,
        operationId);

    auto settleJobRequest = jobTrackerServiceProxy.SettleJob();

    const auto* bootstrap = ControllerAgentConnectorPool_->Bootstrap_;

    SetNodeInfoToRequest(
        bootstrap->GetNodeId(),
        bootstrap->GetLocalDescriptor(),
        settleJobRequest);

    ToProto(settleJobRequest->mutable_controller_agent_incarnation_id(), ControllerAgentDescriptor_.IncarnationId);

    ToProto(settleJobRequest->mutable_allocation_id(), allocationId);
    ToProto(settleJobRequest->mutable_operation_id(), operationId);

    return settleJobRequest->Invoke().ApplyUnique(BIND([
            this,
            this_ = MakeStrong(this),
            allocationId,
            operationId
        ] (TErrorOr<TIntrusivePtr<TTypedClientResponse<TRspSettleJob>>>&& rspOrError) -> TJobStartInfo {
            if (!rspOrError.IsOK()) {
                YT_LOG_DEBUG(
                    rspOrError,
                    "Failed to settle job (ControllerAgentDescriptor: %v, AllocationId: %v, OperationId: %v)",
                    ControllerAgentDescriptor_,
                    allocationId,
                    operationId);

                THROW_ERROR rspOrError;
            }

            auto rsp = std::move(rspOrError.Value());

            YT_LOG_DEBUG(
                "Settle job response received (ControllerAgentDescriptor: %v, AllocationId: %v, OperationId: %v)",
                ControllerAgentDescriptor_,
                allocationId,
                operationId);

            switch (rsp->job_info_or_error_case()) {
                case TRspSettleJob::JobInfoOrErrorCase::kError: {
                    auto error = FromProto<TError>(rsp->error());

                    YT_LOG_DEBUG(
                        error,
                        "No job is available for allocation (ControllerAgentDescriptor: %v, OperationId: %v, AllocationId: %v)",
                        ControllerAgentDescriptor_,
                        operationId,
                        allocationId);

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
                        operationId,
                        allocationId,
                        static_cast<int>(rsp->job_info_or_error_case()));
            }
        })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnConfigUpdated(
    const TControllerAgentConnectorDynamicConfigPtr& newConfig)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    YT_LOG_DEBUG(
        "Set new controller agent heartbeat options (NewPeriod: %v, NewSplay: %v, NewMinBackoff: %v, NewMaxBackoff: %v, NewBackoffMultiplier: %v)",
        newConfig->HeartbeatExecutor.Period,
        newConfig->HeartbeatExecutor.Splay,
        newConfig->HeartbeatExecutor.MinBackoff,
        newConfig->HeartbeatExecutor.MaxBackoff,
        newConfig->HeartbeatExecutor.BackoffMultiplier);

    HeartbeatExecutor_->SetOptions(newConfig->HeartbeatExecutor);
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

TError TControllerAgentConnectorPool::TControllerAgentConnector::DoSendHeartbeat()
{
    const auto* bootstrap = ControllerAgentConnectorPool_->Bootstrap_;

    VERIFY_INVOKER_AFFINITY(bootstrap->GetControlInvoker());

    auto nodeId = bootstrap->GetNodeId();
    auto nodeDescriptor = bootstrap->GetLocalDescriptor();

    if (!bootstrap->IsConnected() || nodeId == InvalidNodeId) {
        return TError();
    }

    TJobTrackerServiceProxy proxy(Channel_);
    auto request = proxy.Heartbeat();

    auto context = New<TAgentHeartbeatContext>();
    auto currentConfig = GetConfig();

    if (currentConfig->TestHeartbeatDelay) {
        TDelayedExecutor::WaitForDuration(currentConfig->TestHeartbeatDelay);
    }

    PrepareHeartbeatRequest(nodeId, nodeDescriptor, request, context);

    auto requestFuture = request->Invoke();
    YT_LOG_INFO(
        "Heartbeat sent to agent (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);

    auto responseOrError = WaitFor(std::move(requestFuture));
    if (!responseOrError.IsOK()) {
        auto [minBackoff, maxBackoff] = HeartbeatExecutor_->GetBackoffInterval();
        YT_LOG_ERROR(
            responseOrError,
            "Error reporting heartbeat to agent (AgentAddress: %v, BackoffTime: [%v, %v])",
            ControllerAgentDescriptor_.Address,
            minBackoff,
            maxBackoff);

        if (responseOrError.GetCode() == NControllerAgent::EErrorCode::IncarnationMismatch) {
            ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker()->Invoke(BIND(
                &TControllerAgentConnector::OnAgentIncarnationOutdated,
                MakeStrong(this)));
        }
        return TError("Failed to report heartbeat to agent");
    }

    auto& response = responseOrError.Value();
    ProcessHeartbeatResponse(response, context);

    YT_LOG_INFO(
        "Successfully reported heartbeat to agent (AgentAddress: %v, IncarnationId: %v)",
        ControllerAgentDescriptor_.Address,
        ControllerAgentDescriptor_.IncarnationId);

    return TError();
}

// This method will be called in control thread when controller agent controls job lifetime.
TError TControllerAgentConnectorPool::TControllerAgentConnector::SendHeartbeat()
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetControlInvoker());

    return DoSendHeartbeat();
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
    context->JobStalenessDelay = GetConfig()->JobStalenessDelay;

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

void TControllerAgentConnectorPool::TControllerAgentConnector::OnAllocationFailed(TAllocationId allocationId)
{
    VERIFY_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    AllocationIdsWaitingForSpec_.erase(allocationId);
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

    jobController->SubscribeAllocationFailed(BIND_NO_PROPAGATE(
            &TControllerAgentConnectorPool::OnAllocationFailed,
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

    THashSet<TControllerAgentConnectorPtr> outdatedControllerAgentConnectors;
    for (const auto& [descriptor, connector] : ControllerAgentConnectors_) {
        if (!controllerAgentDescriptors.contains(descriptor)) {
            YT_LOG_DEBUG(
                "Found outdated controller agent connector, remove it (ControllerAgentDescriptor: %v)",
                descriptor);

            EmplaceOrCrash(outdatedControllerAgentConnectors, connector);
        } else {
            EraseOrCrash(controllerAgentDescriptors, descriptor);
        }
    }

    for (const auto& connector : outdatedControllerAgentConnectors) {
        connector->OnAgentIncarnationOutdated();
    }

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

TFuture<TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo>
TControllerAgentConnectorPool::SettleJob(
    const TControllerAgentDescriptor& agentDescriptor,
    TOperationId operationId,
    TAllocationId allocationId)
{
    VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    auto controllerAgentConnector = GetControllerAgentConnector(agentDescriptor);
    return controllerAgentConnector->SettleJob(operationId, allocationId);
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

void TControllerAgentConnectorPool::OnAllocationFailed(
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
        connectorIt->second->OnAllocationFailed(allocationId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
