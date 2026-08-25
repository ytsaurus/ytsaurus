#include "controller_agent_connector.h"

#include "bootstrap.h"
#include "helpers.h"
#include "job.h"
#include "job_controller.h"
#include "private.h"
#include "throttler_manager.h"

#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/controller_agent/job_tracker_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTracing;
using namespace NNodeTrackerClient;

using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NScheduler::NProto::NNode;

using NScheduler::TIncarnationId;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TControllerAgentConnectorPool::TControllerAgentConnector::TControllerAgentConnector(
    TControllerAgentConnectorPool* controllerAgentConnectorPool,
    TControllerAgentDescriptor controllerAgentDescriptor)
    : ControllerAgentConnectorPool_(MakeStrong(controllerAgentConnectorPool))
    , ControllerAgentDescriptor_(std::move(controllerAgentDescriptor))
    , Channel_(ControllerAgentConnectorPool_->CreateChannel(ControllerAgentDescriptor_))
    , HeartbeatExecutor_(New<TRetryingPeriodicExecutor>(
        ControllerAgentConnectorPool_->Bootstrap_->GetControlInvoker(),
        BIND_NO_PROPAGATE([this, weakThis = MakeWeak(this)] {
            auto this_ = weakThis.Lock();
            return this_ ? SendHeartbeat() : TError("Controller agent connector is destroyed");
        }),
        GetConfig()->HeartbeatExecutor))
    , StatisticsThrottler_(CreateReconfigurableThroughputThrottler(
        GetConfig()->StatisticsThrottler))
{
    if (ControllerAgentConnectorPool_->MasterConnected_) {
        HeartbeatExecutor_->Start();
        YT_TLOG_INFO("Controller agent connector created, starting heartbeats")
            .With("AgentAddress", ControllerAgentDescriptor_.Address)
            .With("IncarnationId", ControllerAgentDescriptor_.IncarnationId)
            .With("HeartbeatOptions", *GetConfig());
    } else {
        YT_TLOG_INFO("Controller agent connector created, waiting for connecting to master")
            .With("AgentAddress", ControllerAgentDescriptor_.Address)
            .With("IncarnationId", ControllerAgentDescriptor_.IncarnationId)
            .With("HeartbeatOptions", *GetConfig());
    }
}

NRpc::IChannelPtr TControllerAgentConnectorPool::TControllerAgentConnector::GetChannel() const noexcept
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Channel_;
}

void TControllerAgentConnectorPool::TControllerAgentConnector::SendOutOfBandHeartbeatIfNeeded()
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    if (ShouldSendOutOfBand_) {
        HeartbeatExecutor_->ScheduleOutOfBand();
        ShouldSendOutOfBand_ = false;
    }
}

void TControllerAgentConnectorPool::TControllerAgentConnector::EnqueueFinishedJob(const TJobPtr& job)
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    EnqueuedFinishedJobs_.insert(job);
    ShouldSendOutOfBand_ = true;
}

const TControllerAgentDescriptor& TControllerAgentConnectorPool::TControllerAgentConnector::GetDescriptor() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return ControllerAgentDescriptor_;
}

void TControllerAgentConnectorPool::TControllerAgentConnector::AddUnconfirmedJobIds(std::vector<TJobId> unconfirmedJobIds)
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    UnconfirmedJobIds_ = std::move(unconfirmedJobIds);
}

TFuture<TControllerAgentConnectorPool::TControllerAgentConnector::TJobStartInfo>
TControllerAgentConnectorPool::TControllerAgentConnector::SettleJob(
    TOperationId operationId,
    TAllocationId allocationId,
    std::optional<TJobId> lastJobId)
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    TJobTrackerServiceProxy jobTrackerServiceProxy(Channel_);

    auto settleJobsTimeout = GetConfig()->SettleJobsTimeout;

    jobTrackerServiceProxy.SetDefaultTimeout(settleJobsTimeout);

    YT_TLOG_DEBUG("Add settle job request")
        .With("AgentDescriptor", ControllerAgentDescriptor_)
        .With("AllocationId", allocationId)
        .With("OperationId", operationId)
        .With("LastJobId", lastJobId);

    auto settleJobRequest = jobTrackerServiceProxy.SettleJob();

    const auto* bootstrap = ControllerAgentConnectorPool_->Bootstrap_;

    SetNodeInfoToRequest(
        bootstrap->GetNodeId(),
        bootstrap->GetLocalDescriptor(),
        settleJobRequest);

    ToProto(settleJobRequest->mutable_controller_agent_incarnation_id(), ControllerAgentDescriptor_.IncarnationId);

    ToProto(settleJobRequest->mutable_allocation_id(), allocationId);
    ToProto(settleJobRequest->mutable_operation_id(), operationId);
    if (lastJobId) {
        ToProto(settleJobRequest->mutable_last_job_id(), *lastJobId);
    }

    return settleJobRequest->Invoke().AsUnique().Apply(BIND([
            this,
            this_ = MakeStrong(this),
            allocationId,
            operationId
        ] (TErrorOr<TIntrusivePtr<TTypedClientResponse<TRspSettleJob>>>&& rspOrError) -> TJobStartInfo {
            if (!rspOrError.IsOK()) {
                YT_TLOG_DEBUG("Failed to settle job")
                    .With("ControllerAgentDescriptor", ControllerAgentDescriptor_)
                    .With("AllocationId", allocationId)
                    .With("OperationId", operationId)
                    .With(rspOrError);

                THROW_ERROR rspOrError;
            }

            auto rsp = std::move(rspOrError.Value());

            YT_TLOG_DEBUG("Settle job response received")
                .With("ControllerAgentDescriptor", ControllerAgentDescriptor_)
                .With("AllocationId", allocationId)
                .With("OperationId", operationId);

            switch (rsp->job_info_or_error_case()) {
                case TRspSettleJob::JobInfoOrErrorCase::kError: {
                    auto error = FromProto<TError>(rsp->error());

                    YT_TLOG_DEBUG("No job is available for allocation")
                        .With("ControllerAgentDescriptor", ControllerAgentDescriptor_)
                        .With("OperationId", operationId)
                        .With("AllocationId", allocationId)
                        .With(error);

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
                    YT_TLOG_FATAL("Unexpected value in job_info_or_error")
                        .With("ControllerAgentDescriptor", ControllerAgentDescriptor_)
                        .With("OperationId", operationId)
                        .With("AllocationId", allocationId)
                        .With("Value", static_cast<int>(rsp->job_info_or_error_case()));
            }
        })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnConfigUpdated(
    const TControllerAgentConnectorDynamicConfigPtr& newConfig)
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    YT_TLOG_DEBUG("Set new controller agent heartbeat options")
        .With("NewHeartbeatOptions", *newConfig);

    HeartbeatExecutor_->SetOptions(newConfig->HeartbeatExecutor);
    StatisticsThrottler_->Reconfigure(newConfig->StatisticsThrottler);
}

TControllerAgentConnectorPool::TControllerAgentConnector::~TControllerAgentConnector()
{
    YT_TLOG_DEBUG("Controller agent connector destroyed")
        .With("AgentAddress", ControllerAgentDescriptor_.Address)
        .With("IncarnationId", ControllerAgentDescriptor_.IncarnationId);

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

    YT_ASSERT_INVOKER_AFFINITY(bootstrap->GetControlInvoker());

    auto nodeId = bootstrap->GetNodeId();
    auto nodeDescriptor = bootstrap->GetLocalDescriptor();

    if (!bootstrap->IsConnected() || nodeId == InvalidNodeId) {
        return TError();
    }

    auto currentConfig = GetConfig();

    TTraceContextPtr requestTraceContext;

    if (currentConfig->EnableTracing) {
        requestTraceContext = TTraceContext::NewRoot("ControllerAgentHeartbeat");
        requestTraceContext->SetRecorded();
        requestTraceContext->AddTag("node_id", nodeId);

        static const std::string ControllerAgentConnectorTracingUserName = "controller_agent_connector";
        ControllerAgentConnectorPool_->TracingSampler_->SampleTraceContext(ControllerAgentConnectorTracingUserName, requestTraceContext);
    }

    auto contextGuard = TTraceContextGuard(requestTraceContext);

    TJobTrackerServiceProxy proxy(Channel_);
    auto request = proxy.Heartbeat();
    request->SetTimeout(currentConfig->HeartbeatTimeout);

    auto context = New<TAgentHeartbeatContext>();

    if (currentConfig->TestHeartbeatDelay) {
        TDelayedExecutor::WaitForDuration(currentConfig->TestHeartbeatDelay);
    }

    PrepareHeartbeatRequest(nodeId, nodeDescriptor, request, context);

    YT_TLOG_INFO("Heartbeat sent to agent")
        .With("AgentAddress", ControllerAgentDescriptor_.Address)
        .With("IncarnationId", ControllerAgentDescriptor_.IncarnationId);

    {
        request->set_sequence_number(SequenceNumber_);
        ++SequenceNumber_;
    }
    auto responseOrError = WaitFor(std::move(request->Invoke()));
    if (!responseOrError.IsOK()) {
        auto [minBackoff, maxBackoff] = HeartbeatExecutor_->GetBackoffInterval();
        YT_TLOG_ERROR("Error reporting heartbeat to agent")
            .With("AgentAddress", ControllerAgentDescriptor_.Address)
            .WithFormat("BackoffTime", "[%v, %v]", minBackoff, maxBackoff)
            .With(responseOrError);

        if (responseOrError.GetCode() == NControllerAgent::EErrorCode::IncarnationMismatch) {
            ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker()->Invoke(BIND(
                &TControllerAgentConnector::OnAgentIncarnationOutdated,
                MakeStrong(this)));
        }
        return TError("Failed to report heartbeat to agent");
    }

    auto& response = responseOrError.Value();
    ProcessHeartbeatResponse(response, context);

    YT_TLOG_INFO("Successfully reported heartbeat to agent")
        .With("AgentAddress", ControllerAgentDescriptor_.Address)
        .With("IncarnationId", ControllerAgentDescriptor_.IncarnationId);

    return TError();
}

// This method will be called in control thread when controller agent controls job lifetime.
TError TControllerAgentConnectorPool::TControllerAgentConnector::SendHeartbeat()
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetControlInvoker());

    return DoSendHeartbeat();
}

void TControllerAgentConnectorPool::TControllerAgentConnector::PrepareHeartbeatRequest(
    TNodeId nodeId,
    const TNodeDescriptor& nodeDescriptor,
    const TReqHeartbeatPtr& request,
    const TAgentHeartbeatContextPtr& context)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    const auto* bootstrap = ControllerAgentConnectorPool_->Bootstrap_;

    SetNodeInfoToRequest(
        nodeId,
        nodeDescriptor,
        request);

    auto clusterTrafficAvailability = bootstrap->GetExecNodeBootstrap()->GetThrottlerManager()->GetClusterToIncomingTrafficAvailability(EThrottlerTrafficType::Bandwidth);
    if (clusterTrafficAvailability) {
        // I (this exe node) am the leader and so I am responsible for sending bandwidth availability to controller agents.
        auto* cluster_network_bandwidth_availability = request->mutable_cluster_network_bandwidth_availability();
        for (const auto& [clusterName, isAvailable] : *clusterTrafficAvailability) {
            if (isAvailable) {
                // Skip available clusters.
                continue;
            }

            auto* availability = cluster_network_bandwidth_availability->add_availability();
            availability->set_cluster_name(*clusterName.Underlying());
            availability->set_is_available(isAvailable);
        }
    }

    auto error = WaitFor(BIND(
            &TControllerAgentConnector::DoPrepareHeartbeatRequest,
            MakeStrong(this),
            request,
            context)
        .AsyncVia(bootstrap->GetJobInvoker())
        .Run());

    YT_TLOG_FATAL_IF(!error.IsOK(), "Failed to prepare agent heartbeat request")
        .With(error);
}

void TControllerAgentConnectorPool::TControllerAgentConnector::ProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    const TAgentHeartbeatContextPtr& context)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto error = WaitFor(BIND(
            &TControllerAgentConnector::DoProcessHeartbeatResponse,
            MakeStrong(this),
            response,
            context)
        .AsyncVia(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker())
        .Run());

    YT_TLOG_FATAL_IF(!error.IsOK(), "Failed to process agent heartbeat response")
        .With(error);
}

void TControllerAgentConnectorPool::TControllerAgentConnector::DoPrepareHeartbeatRequest(
    const TReqHeartbeatPtr& request,
    const TAgentHeartbeatContextPtr& context)
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    context->ControllerAgentConnector = MakeStrong(this);
    context->StatisticsThrottler = StatisticsThrottler_;
    context->RunningJobStatisticsSendingBackoff = GetConfig()->RunningJobStatisticsSendingBackoff;
    context->JobStalenessDelay = GetConfig()->JobStalenessDelay;
    context->ResendFullJobInfo = GetConfig()->ResendFullJobInfo;

    context->JobsToForcefullySend = EnqueuedFinishedJobs_;
    context->UnconfirmedJobIds = std::move(UnconfirmedJobIds_);

    const auto& jobController = ControllerAgentConnectorPool_->Bootstrap_->GetJobController();
    jobController->PrepareAgentHeartbeatRequest(request, context);
}

void TControllerAgentConnectorPool::TControllerAgentConnector::DoProcessHeartbeatResponse(
    const TRspHeartbeatPtr& response,
    const TAgentHeartbeatContextPtr& context)
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    const auto& jobController = ControllerAgentConnectorPool_->Bootstrap_->GetJobController();
    jobController->ProcessAgentHeartbeatResponse(response, context);

    for (auto finishedJobId : context->JobsToForcefullySend) {
        EnqueuedFinishedJobs_.erase(finishedJobId);
    }
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnMasterConnected()
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    YT_TLOG_INFO("Starting heartbeats to controller agent")
        .With("AgentDescriptor", ControllerAgentDescriptor_);

    HeartbeatExecutor_->Start();
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnMasterDisconnected()
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    YT_TLOG_INFO("Stopping heartbeats to controller agent")
        .With("AgentDescriptor", ControllerAgentDescriptor_);

    YT_UNUSED_FUTURE(HeartbeatExecutor_->Stop());
}

void TControllerAgentConnectorPool::TControllerAgentConnector::OnAgentIncarnationOutdated() noexcept
{
    YT_ASSERT_INVOKER_AFFINITY(ControllerAgentConnectorPool_->Bootstrap_->GetJobInvoker());

    YT_TLOG_DEBUG("Controller agent incarnation is outdated, stop connector")
        .With("ControllerAgentDescriptor", ControllerAgentDescriptor_);

    const auto& jobController = ControllerAgentConnectorPool_->Bootstrap_->GetJobController();
    jobController->OnAgentIncarnationOutdated(ControllerAgentDescriptor_);

    EnqueuedFinishedJobs_.clear();

    YT_UNUSED_FUTURE(HeartbeatExecutor_->Stop());

    if (ControllerAgentConnectorPool_->ControllerAgentConnectors_.erase(ControllerAgentDescriptor_)) {
        YT_TLOG_DEBUG("Remove controller agent connector since incarnation is outdated")
            .With("ControllerAgentDescriptor", ControllerAgentDescriptor_);

        ControllerAgentConnectorPool_->IncrementEpoch();
    } else {
        YT_TLOG_DEBUG("Controller agent connector is already removed")
            .With("ControllerAgentDescriptor", ControllerAgentDescriptor_);
    }
}

////////////////////////////////////////////////////////////////////////////////

TControllerAgentConnectorPool::TControllerAgentConnectorPool(
    IBootstrap* const bootstrap)
    : DynamicConfig_(New<TControllerAgentConnectorDynamicConfig>())
    , Bootstrap_(bootstrap)
    , TracingSampler_(New<TSampler>(
        DynamicConfig_.Acquire()->TracingSampler,
        ControllerAgentConnectorProfiler().WithPrefix("/tracing")))
{ }

void TControllerAgentConnectorPool::Initialize()
{
    const auto& jobController = Bootstrap_->GetJobController();

    jobController->SubscribeJobFinished(BIND_NO_PROPAGATE(
            &TControllerAgentConnectorPool::OnJobFinished,
            MakeWeak(this))
        .Via(Bootstrap_->GetJobInvoker()));

    const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
    clusterNodeMasterConnector->SubscribeMasterConnected(BIND_NO_PROPAGATE(
        &TControllerAgentConnectorPool::OnMasterConnected,
        MakeWeak(this)));
    clusterNodeMasterConnector->SubscribeMasterDisconnected(BIND_NO_PROPAGATE(
        &TControllerAgentConnectorPool::OnMasterDisconnected,
        MakeWeak(this)));
}

void TControllerAgentConnectorPool::Start()
{ }

void TControllerAgentConnectorPool::SendOutOfBandHeartbeatsIfNeeded()
{
    for (auto& [agentDescriptor, controllerAgentConnector] : ControllerAgentConnectors_) {
        controllerAgentConnector->SendOutOfBandHeartbeatIfNeeded();
    }
}

void TControllerAgentConnectorPool::OnDynamicConfigChanged(
    const TControllerAgentConnectorDynamicConfigPtr& /*oldConfig*/,
    const TControllerAgentConnectorDynamicConfigPtr& newConfig)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

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
    YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    YT_TLOG_DEBUG("Received registered controller agents")
        .With("ControllerAgentCount", std::size(controllerAgentDescriptors));

    THashSet<TControllerAgentConnectorPtr> outdatedControllerAgentConnectors;
    for (const auto& [descriptor, connector] : ControllerAgentConnectors_) {
        if (!controllerAgentDescriptors.contains(descriptor)) {
            YT_TLOG_DEBUG("Found outdated controller agent connector, remove it")
                .With("ControllerAgentDescriptor", descriptor);

            EmplaceOrCrash(outdatedControllerAgentConnectors, connector);
        } else {
            EraseOrCrash(controllerAgentDescriptors, descriptor);
        }
    }

    for (const auto& connector : outdatedControllerAgentConnectors) {
        connector->OnAgentIncarnationOutdated();
    }

    for (auto& descriptor : controllerAgentDescriptors) {
        YT_TLOG_DEBUG("Add new controller agent connector")
            .With("ControllerAgentDescriptor", descriptor);
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

std::optional<TControllerAgentDescriptor> TControllerAgentConnectorPool::FindDescriptorByIncarnationId(TIncarnationId incarnationId) const
{
    for (const auto& [descriptor, _] : ControllerAgentConnectors_) {
        if (descriptor.IncarnationId == incarnationId) {
            return descriptor;
        }
    }
    return std::nullopt;
}

TControllerAgentDescriptor TControllerAgentConnectorPool::GetDescriptorByIncarnationId(TIncarnationId incarnationId) const
{
    auto result = FindDescriptorByIncarnationId(incarnationId);
    YT_VERIFY(result);

    return *result;
}

void TControllerAgentConnectorPool::OnMasterConnected()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    Bootstrap_->GetJobInvoker()->Invoke(BIND([this, this_ = MakeStrong(this)] {
        MasterConnected_ = true;
        for (const auto& [descriptor, connector] : ControllerAgentConnectors_) {
            connector->OnMasterConnected();
        }
    }));
}

void TControllerAgentConnectorPool::OnMasterDisconnected()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    Bootstrap_->GetJobInvoker()->Invoke(BIND([this, this_ = MakeStrong(this)] {
        MasterConnected_ = false;
        for (const auto& [descriptor, connector] : ControllerAgentConnectors_) {
            connector->OnMasterDisconnected();
        }
    }));
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
    YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

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
    YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    for (const auto& [agentDescriptor, controllerAgentConnector] : ControllerAgentConnectors_) {
        controllerAgentConnector->OnConfigUpdated(newConfig);
    }

    TracingSampler_->UpdateConfig(newConfig->TracingSampler);
}

void TControllerAgentConnectorPool::OnJobFinished(const TJobPtr& job)
{
    if (auto controllerAgentConnector = job->GetControllerAgentConnector()) {
        controllerAgentConnector->EnqueueFinishedJob(job);

        YT_TLOG_DEBUG("Job finished, send out of band heartbeat to controller agent")
            .With("JobId", job->GetId())
            .With("ControllerAgentDescriptor", controllerAgentConnector->GetDescriptor());

        controllerAgentConnector->SendOutOfBandHeartbeatIfNeeded();
    }
}

int TControllerAgentConnectorPool::GetEpoch() const
{
    YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
    return Epoch_;
}

void TControllerAgentConnectorPool::IncrementEpoch()
{
    YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);
    ++Epoch_;
}

TWeakPtr<TControllerAgentConnectorPool::TControllerAgentConnector>
TControllerAgentConnectorPool::AddControllerAgentConnector(
    TControllerAgentDescriptor agentDescriptor)
{
    YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    auto controllerAgentConnector = New<TControllerAgentConnector>(this, agentDescriptor);

    EmplaceOrCrash(ControllerAgentConnectors_, std::move(agentDescriptor), controllerAgentConnector);

    IncrementEpoch();

    return controllerAgentConnector;
}

TIntrusivePtr<TControllerAgentConnectorPool::TControllerAgentConnector>
TControllerAgentConnectorPool::GetControllerAgentConnector(
    const TControllerAgentDescriptor& agentDescriptor)
{
    YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

    if (!agentDescriptor) {
        return nullptr;
    }

    return GetOrCrash(ControllerAgentConnectors_, agentDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
