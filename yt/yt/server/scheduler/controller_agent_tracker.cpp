#include "controller_agent_tracker.h"

#include "scheduler.h"
#include "scheduler_strategy.h"
#include "controller_agent.h"
#include "operation.h"
#include "node_manager.h"
#include "operation_controller_impl.h"
#include "scheduling_context.h"
#include "master_connector.h"
#include "bootstrap.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/build/build.h>

#include <util/string/join.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NControllerAgent;
using namespace NTracing;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TOperationInfo
{
    TOperationId OperationId;
    TOperationJobMetrics JobMetrics;
    THashMap<EOperationAlertType, TError> AlertMap;
    TControllerRuntimeDataPtr ControllerRuntimeData;
    TYsonString SuspiciousJobsYson;
};

void FromProto(TOperationInfo* operationInfo, const NProto::TOperationInfo& operationInfoProto)
{
    operationInfo->OperationId = FromProto<TOperationId>(operationInfoProto.operation_id());
    operationInfo->JobMetrics = FromProto<TOperationJobMetrics>(operationInfoProto.job_metrics());
    if (operationInfoProto.has_alerts()) {
        THashMap<EOperationAlertType, TError> alertMap;
        for (const auto& protoAlert : operationInfoProto.alerts().alerts()) {
            alertMap[EOperationAlertType(protoAlert.type())] = FromProto<TError>(protoAlert.error());
        }
        operationInfo->AlertMap = std::move(alertMap);
    }

    if (operationInfoProto.has_suspicious_jobs()) {
        operationInfo->SuspiciousJobsYson = TYsonString(operationInfoProto.suspicious_jobs(), EYsonType::MapFragment);
    } else {
        operationInfo->SuspiciousJobsYson = TYsonString();
    }

    auto controllerData = New<TControllerRuntimeData>();

    TCompositeNeededResources neededResources;
    FromProto(&neededResources, operationInfoProto.composite_needed_resources());
    controllerData->SetNeededResources(std::move(neededResources));

    controllerData->MinNeededResources() = FromProto<TJobResourcesWithQuotaList>(operationInfoProto.min_needed_resources());
    operationInfo->ControllerRuntimeData = std::move(controllerData);
}

////////////////////////////////////////////////////////////////////////////////

void ProcessScheduleAllocationMailboxes(
    const TControllerAgentTracker::TCtxAgentScheduleAllocationHeartbeatPtr& context,
    const TControllerAgentPtr& agent,
    const TNodeManagerPtr& nodeManager,
    std::vector<std::vector<const NProto::TScheduleAllocationResponse*>>& groupedScheduleAllocationResponses)
{
    auto* request = &context->Request();
    auto* response = &context->Response();

    const auto Logger = SchedulerLogger
        .WithTag("RequestId: %v, IncarnationId: %v", context->GetRequestId(), request->agent_id());

    YT_LOG_DEBUG("Processing schedule allocation mailboxes");

    agent->GetScheduleAllocationResponsesInbox()->HandleIncoming(
        request->mutable_agent_to_scheduler_schedule_allocation_responses(),
        [&] (auto* protoEvent) {
            auto allocationId = FromProto<TAllocationId>(protoEvent->allocation_id());
            auto shardId = nodeManager->GetNodeShardId(NodeIdFromAllocationId(allocationId));
            groupedScheduleAllocationResponses[shardId].push_back(protoEvent);
        });
    agent->GetScheduleAllocationResponsesInbox()->ReportStatus(
        response->mutable_agent_to_scheduler_schedule_allocation_responses());

    agent->GetScheduleAllocationRequestsOutbox()->HandleStatus(
        request->scheduler_to_agent_schedule_allocation_requests());
    agent->GetScheduleAllocationRequestsOutbox()->BuildOutcoming(
        response->mutable_scheduler_to_agent_schedule_allocation_requests(),
        [] (auto* protoRequest, const auto& request) {
            ToProto(protoRequest, *request);
        });

    YT_LOG_DEBUG("Schedule allocation mailboxes processed");
}

void ProcessScheduleAllocationResponses(
    TControllerAgentTracker::TCtxAgentScheduleAllocationHeartbeatPtr context,
    const std::vector<TNodeShardPtr>& nodeShards,
    const std::vector<IInvokerPtr>& nodeShardInvokers,
    std::vector<std::vector<const NProto::TScheduleAllocationResponse*>> groupedScheduleAllocationResponses,
    const IInvokerPtr& dtorInvoker)
{
    auto Logger = SchedulerLogger
        .WithTag("RequestId: %v, IncarnationId: %v", context->GetRequestId(), context->Request().agent_id());

    YT_LOG_DEBUG("Processing schedule allocation responses");

    std::vector<TFuture<void>> futures;
    for (int shardId = 0; shardId < std::ssize(nodeShards); ++shardId) {
        futures.push_back(
            BIND([
                context,
                nodeShard = nodeShards[shardId],
                protoResponses = std::move(groupedScheduleAllocationResponses[shardId]),
                Logger = SchedulerLogger
            ] {
                for (const auto* protoResponse : protoResponses) {
                    auto operationId = FromProto<TOperationId>(protoResponse->operation_id());
                    auto allocationId = FromProto<TAllocationId>(protoResponse->allocation_id());
                    auto controllerEpoch = TControllerEpoch(protoResponse->controller_epoch());
                    auto expectedControllerEpoch = nodeShard->GetOperationControllerEpoch(operationId);

                    auto traceContext = TTraceContext::NewChildFromRpc(
                        protoResponse->tracing_ext(),
                        /*spanName*/ Format("ScheduleAllocation:%v", allocationId),
                        context->GetRequestId(),
                        /*forceTracing*/ false);

                    {
                        TCurrentTraceContextGuard traceContextGuard(traceContext);

                        if (controllerEpoch != expectedControllerEpoch) {
                            YT_LOG_DEBUG(
                                "Received allocation schedule result with unexpected controller epoch; result is ignored "
                                "(OperationId: %v, AllocationId: %v, ControllerEpoch: %v, ExpectedControllerEpoch: %v)",
                                operationId,
                                allocationId,
                                controllerEpoch,
                                expectedControllerEpoch);
                            continue;
                        }
                        if (nodeShard->IsOperationControllerTerminated(operationId)) {
                            YT_LOG_DEBUG(
                                "Received allocation schedule result for operation whose controller is terminated; "
                                "result is ignored (OperationId: %v, AllocationId: %v)",
                                operationId,
                                allocationId);
                            continue;
                        }
                        nodeShard->EndScheduleAllocation(*protoResponse);
                    }
                }
            })
            .AsyncVia(nodeShardInvokers[shardId])
            .Run());
    }

    AllSet(std::move(futures))
        .Subscribe(
            BIND([context = std::move(context)] (const TError&) {
                auto request = std::move(context->Request());
                Y_UNUSED(request);
            })
            .Via(dtorInvoker));

    YT_LOG_DEBUG("Schedule allocation responses are processed");
}

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTracker::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : SchedulerConfig_(std::move(config))
        , Config_(SchedulerConfig_->ControllerAgentTracker)
        , AtomicConfig_(Config_)
        , Bootstrap_(bootstrap)
        , MessageOffloadThreadPool_(CreateThreadPool(Config_->MessageOffloadThreadCount, "MsgOffload"))
        , HeartbeatActionQueue_(New<TActionQueue>("ControllerAgent"))
        , ResponseKeeper_(CreateResponseKeeper(
            Config_->ResponseKeeper,
            Bootstrap_->GetControlInvoker(EControlQueue::AgentTracker),
            SchedulerLogger,
            SchedulerProfiler))
    { }

    void Initialize()
    {
        auto* masterConnector = Bootstrap_->GetScheduler()->GetMasterConnector();
        masterConnector->SubscribeMasterConnected(BIND_NO_PROPAGATE(
            &TImpl::OnMasterConnected,
            Unretained(this)));
        masterConnector->SubscribeMasterDisconnected(BIND_NO_PROPAGATE(
            &TImpl::OnMasterDisconnected,
            Unretained(this)));

        masterConnector->AddCommonWatcher(
            BIND_NO_PROPAGATE(&TImpl::RequestControllerAgentInstances, Unretained(this)),
            BIND_NO_PROPAGATE(&TImpl::HandleControllerAgentInstances, Unretained(this)));
    }

    std::vector<TControllerAgentPtr> GetAgents() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TControllerAgentPtr> result;
        result.reserve(IdToAgent_.size());
        for (const auto& [agentId, agent] : IdToAgent_) {
            result.push_back(agent);
        }
        return result;
    }

    IOperationControllerPtr CreateController(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return New<TOperationControllerImpl>(Bootstrap_, SchedulerConfig_, operation);
    }

    TControllerAgentPtr PickAgentForOperation(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto controllerAgentTag = operation->GetRuntimeParameters()->ControllerAgentTag;

        if (!AgentTagsFetched_ || TagsWithTooFewAgents_.contains(controllerAgentTag)) {
            YT_LOG_INFO(
                "Failed to pick agent since number of agent with matching tag is too low (OperationId: %v, ControllerAgentTag: %v)",
                operation->GetId(),
                controllerAgentTag);

            return nullptr;
        }

        int nonMatchingTagCount = 0;
        int nonRegisteredCount = 0;
        int missingMemoryStatisticsCount = 0;
        int notEnoughMemoryCount = 0;

        std::vector<TControllerAgentPtr> aliveAgents;
        for (const auto& [agentId, agent] : IdToAgent_) {
            // (*) Only possible concurrent mutation to GetState is transition
            // WaitingForFirstHeartbeat -> Registered.
            // Access to GetState itself is atomic, so no race here.
            // (*) Implies that agent considered alive cannot leave such state
            // concurrently to this function execution.
            if (agent->GetState() != EControllerAgentState::Registered) {
                ++nonRegisteredCount;
                continue;
            }
            if (!agent->GetTags().contains(controllerAgentTag)) {
                ++nonMatchingTagCount;
                continue;
            }
            aliveAgents.push_back(agent);
        }

        TControllerAgentPtr pickedAgent = nullptr;

        switch (Config_->AgentPickStrategy) {
            case EControllerAgentPickStrategy::Random: {
                std::vector<TControllerAgentPtr> agents;
                for (const auto& agent : aliveAgents) {
                    auto memoryStatistics = agent->GetMemoryStatistics();
                    if (memoryStatistics) {
                        auto minAgentAvailableMemory = std::max(
                            Config_->MinAgentAvailableMemory,
                            static_cast<i64>(Config_->MinAgentAvailableMemoryFraction * memoryStatistics->Limit));
                        if (memoryStatistics->Usage + minAgentAvailableMemory >= memoryStatistics->Limit) {
                            ++notEnoughMemoryCount;
                            continue;
                        }
                    }
                    agents.push_back(agent);
                }

                if (!agents.empty()) {
                    pickedAgent = agents[RandomNumber(agents.size())];
                }
                break;
            }
            case EControllerAgentPickStrategy::MemoryUsageBalanced: {
                double scoreSum = 0.0;
                for (const auto& agent : aliveAgents) {
                    auto memoryStatistics = agent->GetMemoryStatistics();
                    if (!memoryStatistics) {
                        ++missingMemoryStatisticsCount;
                        YT_LOG_WARNING("Controller agent skipped since it did not report memory information "
                            "and memory usage balanced pick strategy used (AgentId: %v)",
                            agent->GetId());
                        continue;
                    }

                    auto minAgentAvailableMemory = std::max(
                        Config_->MinAgentAvailableMemory,
                        static_cast<i64>(Config_->MinAgentAvailableMemoryFraction * memoryStatistics->Limit));
                    if (memoryStatistics->Usage + minAgentAvailableMemory >= memoryStatistics->Limit) {
                        ++notEnoughMemoryCount;
                        continue;
                    }

                    i64 freeMemory = std::max(static_cast<i64>(0), memoryStatistics->Limit - memoryStatistics->Usage);
                    double rawScore = static_cast<double>(freeMemory) / memoryStatistics->Limit;
                    double score = std::pow(rawScore, Config_->MemoryBalancedPickStrategyScorePower);

                    scoreSum += score;
                    if (RandomNumber<float>() <= static_cast<float>(score) / scoreSum) {
                        pickedAgent = agent;
                    }
                }
                break;
            }
            default: {
                YT_ABORT();
            }
        }

        if (!pickedAgent) {
            YT_LOG_INFO(
                "Failed to pick agent for operation ("
                "OperationId: %v, ControllerAgentTag: %v, "
                "NonMatchingTagCount: %v, NonRegisteredCount: %v, "
                "MissingMemoryStatisticsCount: %v, NotEnoughMemoryCount: %v)",
                operation->GetId(),
                controllerAgentTag,
                nonMatchingTagCount,
                nonRegisteredCount,
                missingMemoryStatisticsCount,
                notEnoughMemoryCount);
        }

        return pickedAgent;
    }

    void AssignOperationToAgent(
        const TOperationPtr& operation,
        const TControllerAgentPtr& agent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(agent->Operations().insert(operation).second);
        operation->SetAgent(agent.Get());

        YT_LOG_INFO("Operation assigned to agent (AgentId: %v, Tags: %v, OperationId: %v)",
            agent->GetId(),
            agent->GetTags(),
            operation->GetId());
    }


    void HandleAgentFailure(
        const TControllerAgentPtr& agent,
        const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_WARNING(error, "Agent failed; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        Bootstrap_
            ->GetControlInvoker(EControlQueue::AgentTracker)
            ->Invoke(BIND([this, this_ = MakeStrong(this), agent] {
                auto agentGuard = agent->AcquireInnerStateLock();
                UnregisterAgent(agent, std::move(agentGuard));
            }));
    }


    void UnregisterOperationFromAgent(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = operation->FindAgent();
        if (!agent) {
            return;
        }

        EraseOrCrash(agent->Operations(), operation);

        YT_LOG_DEBUG("Operation unregistered from agent (AgentId: %v, OperationId: %v)",
            agent->GetId(),
            operation->GetId());
    }

    TControllerAgentTrackerConfigPtr GetConfig() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return AtomicConfig_.Acquire();
    }

    void UpdateConfig(TSchedulerConfigPtr config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SchedulerConfig_ = std::move(config);
        Config_ = SchedulerConfig_->ControllerAgentTracker;
        AtomicConfig_.Store(Config_);

        MessageOffloadThreadPool_->Configure(Config_->MessageOffloadThreadCount);
    }

    const IResponseKeeperPtr& GetResponseKeeper() const
    {
        return ResponseKeeper_;
    }

    IInvokerPtr GetHeartbeatInvoker() const
    {
        return HeartbeatActionQueue_->GetInvoker();
    }

    TControllerAgentPtr DoFindAgent(
        const TAgentId id,
        const THashMap<TAgentId, TControllerAgentPtr>& idToAgent) const
    {
        auto it = idToAgent.find(id);
        return it == idToAgent.end() ? nullptr : it->second;
    }

    TControllerAgentPtr FindAgent(const TAgentId& id) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return DoFindAgent(id, IdToAgent_);
    }

    TControllerAgentPtr GetAgentOrThrow(const TAgentId& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = FindAgent(id);
        if (!agent) {
            THROW_ERROR_EXCEPTION(
                "Agent %v is not registered",
                id);
        }
        return agent;
    }

    TControllerAgentPtr GetMirroredAgentOrThrow(const TAgentId& id)
    {
        VERIFY_INVOKER_AFFINITY(GetHeartbeatInvoker());

        auto agent = DoFindAgent(id, MirroredIdToAgent_);
        if (!agent) {
            THROW_ERROR_EXCEPTION(
                "Agent %v is not registered",
                id);
        }
        return agent;
    }

    template <CInvocable<void(THashMap<TAgentId, TControllerAgentPtr>&)> TMutator>
    void MutateAgentMappings(TMutator mutator)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        mutator(IdToAgent_);

        GetHeartbeatInvoker()->Invoke(BIND([mutator = std::move(mutator), this, this_ = MakeStrong(this)] () mutable {
            mutator(MirroredIdToAgent_);
        }));
    }

    void HandleAgentAndRenewLease(const TControllerAgentPtr& agent, TIncarnationId incarnationId)
    {
        auto agentGuard = agent->AcquireInnerStateLock();

        auto agentState = agent->GetState();
        if (
            agentState != EControllerAgentState::Registered &&
            agentState != EControllerAgentState::WaitingForInitialHeartbeat
        ) {
            THROW_ERROR_EXCEPTION(
                "Agent %Qv is in %Qlv state",
                agent->GetId(),
                agentState);
        }
        if (incarnationId != agent->GetIncarnationId()) {
            THROW_ERROR_EXCEPTION(
                "Wrong agent incarnation id: expected %v, got %v",
                agent->GetIncarnationId(),
                incarnationId);
        }
        if (agentState == EControllerAgentState::WaitingForInitialHeartbeat) {
            YT_LOG_INFO("Agent registration confirmed by heartbeat");
            agent->SetState(EControllerAgentState::Registered);
        }

        TLeaseManager::RenewLease(agent->GetLease(), Config_->HeartbeatTimeout);
    }

    void ProcessOperationInfos(
        const TCtxAgentHeartbeatPtr& context,
        const TControllerAgentPtr& agent)
    {
        VERIFY_INVOKER_AFFINITY(GetHeartbeatInvoker());

        const auto& scheduler = Bootstrap_->GetScheduler();

        auto* request = &context->Request();
        auto* response = &context->Response();

        std::vector<TOperationInfo> operationInfos;
        operationInfos.reserve(request->operations().size());
        for (const auto& operationInfoProto : request->operations()) {
            operationInfos.emplace_back(FromProto<TOperationInfo>(operationInfoProto));
        }

        agent->GetCancelableControlInvoker()->Invoke(BIND([
                scheduler,
                response,
                agentId = agent->GetId(),
                operationInfos = std::move(operationInfos)
            ] {
                RunNoExcept([&] {
                    TOperationIdToOperationJobMetrics operationIdToOperationJobMetrics;
                    for (const auto& operationInfo : operationInfos) {
                        auto operationId = operationInfo.OperationId;
                        auto operation = scheduler->FindOperation(operationId);
                        if (!operation) {
                            // TODO(eshcherbin): This is used for flap diagnostics. Remove when TestPoolMetricsPorto is fixed (YT-12207).
                            THashMap<TString, i64> treeIdToOperationTotalTimeDelta;
                            for (const auto& [treeId, metrics] : operationInfo.JobMetrics) {
                                treeIdToOperationTotalTimeDelta.emplace(treeId, metrics.Values()[EJobMetricName::TotalTime]);
                            }

                            YT_LOG_DEBUG(
                                "Unknown operation is running at agent; unregister requested (AgentId: %v, OperationId: %v, TreeIdToOperationTotalTimeDelta: %v)",
                                agentId,
                                operationId,
                                treeIdToOperationTotalTimeDelta);
                            ToProto(response->add_operation_ids_to_unregister(), operationId);
                            continue;
                        }
                        EmplaceOrCrash(operationIdToOperationJobMetrics, operationId, std::move(operationInfo.JobMetrics));

                        for (const auto& [alertType, alert] : operationInfo.AlertMap) {
                            YT_UNUSED_FUTURE(scheduler->SetOperationAlert(operationId, alertType, alert));
                        }

                        if (operationInfo.SuspiciousJobsYson) {
                            operation->SetSuspiciousJobs(operationInfo.SuspiciousJobsYson);
                        }

                        auto controllerRuntimeDataError = CheckControllerRuntimeData(operationInfo.ControllerRuntimeData);
                        if (controllerRuntimeDataError.IsOK()) {
                            operation->GetController()->SetControllerRuntimeData(operationInfo.ControllerRuntimeData);
                            YT_UNUSED_FUTURE(scheduler->SetOperationAlert(operationId, EOperationAlertType::InvalidControllerRuntimeData, TError()));
                        } else {
                            auto error = TError("Controller agent reported invalid data for operation")
                                << TErrorAttribute("operation_id", operation->GetId())
                                << std::move(controllerRuntimeDataError);
                            YT_UNUSED_FUTURE(scheduler->SetOperationAlert(operationId, EOperationAlertType::InvalidControllerRuntimeData, error));
                        }
                    }

                    scheduler->GetStrategy()->ApplyJobMetricsDelta(std::move(operationIdToOperationJobMetrics));
                });
            }));
    }

    void HandleOperationEventsInbox(
        const TCtxAgentHeartbeatPtr& context,
        const TControllerAgentPtr& agent)
    {
        const auto& scheduler = Bootstrap_->GetScheduler();

        auto* request = &context->Request();
        auto* response = &context->Response();

        agent->GetCancelableControlInvoker()->Invoke(BIND([
                this,
                this_ = MakeStrong(this),
                agent,
                scheduler,
                requestEvents = std::move(*request->mutable_agent_to_scheduler_operation_events()),
                responseEvents = std::move(*response->mutable_agent_to_scheduler_operation_events())
            ] () mutable {
                // NB: OnInitializationFinished, OnPreparationFinished, OnMaterializationFinished, OnRevivalFinished and OnCommitFinished
                // can in fact throw if agent pointer expires. However, we have it in our closure meaning that
                // we are guaranteed to have agent alive at least while this lambda is alive.
                RunNoExcept([&] {
                    YT_LOG_DEBUG("Handling operation events inbox");
                    agent->GetOperationEventsInbox()->HandleIncoming(
                        &requestEvents,
                        [&] (auto* protoEvent) {
                            auto eventType = static_cast<EAgentToSchedulerOperationEventType>(protoEvent->event_type());
                            auto operationId = FromProto<TOperationId>(protoEvent->operation_id());
                            auto controllerEpoch = TControllerEpoch(protoEvent->controller_epoch());
                            auto error = FromProto<TError>(protoEvent->error());

                            auto operation = scheduler->FindOperation(operationId);
                            if (!operation) {
                                return;
                            }

                            if (operation->ControllerEpoch() != controllerEpoch) {
                                YT_LOG_DEBUG("Received operation event with unexpected controller epoch; ignored "
                                    "(OperationId: %v, ControllerEpoch: %v, EventType: %v)",
                                    operationId,
                                    controllerEpoch,
                                    eventType);
                                return;
                            }

                            switch (eventType) {
                                case EAgentToSchedulerOperationEventType::Completed:
                                    scheduler->OnOperationCompleted(operation);
                                    break;
                                case EAgentToSchedulerOperationEventType::Suspended:
                                    scheduler->OnOperationSuspended(operation, error);
                                    break;
                                case EAgentToSchedulerOperationEventType::Aborted:
                                    scheduler->OnOperationAborted(operation, error);
                                    break;
                                case EAgentToSchedulerOperationEventType::Failed:
                                    scheduler->OnOperationFailed(operation, error);
                                    break;
                                case EAgentToSchedulerOperationEventType::BannedInTentativeTree: {
                                    auto treeId = protoEvent->tentative_tree_id();
                                    auto allocationIds = FromProto<std::vector<TAllocationId>>(protoEvent->tentative_tree_allocation_ids());
                                    scheduler->OnOperationBannedInTentativeTree(operation, treeId, allocationIds);
                                    break;
                                }
                                case EAgentToSchedulerOperationEventType::InitializationFinished: {
                                    TErrorOr<TOperationControllerInitializeResult> resultOrError;
                                    if (error.IsOK()) {
                                        YT_ASSERT(protoEvent->has_initialize_result());

                                        TOperationControllerInitializeResult result;
                                        FromProto(
                                            &result,
                                            protoEvent->initialize_result(),
                                            operationId,
                                            Bootstrap_,
                                            SchedulerConfig_->OperationTransactionPingPeriod);

                                        resultOrError = std::move(result);
                                    } else {
                                        resultOrError = std::move(error);
                                    }

                                    operation->GetController()->OnInitializationFinished(resultOrError);
                                    break;
                                }
                                case EAgentToSchedulerOperationEventType::PreparationFinished: {
                                    TErrorOr<TOperationControllerPrepareResult> resultOrError;
                                    if (error.IsOK()) {
                                        YT_ASSERT(protoEvent->has_prepare_result());
                                        resultOrError = FromProto<TOperationControllerPrepareResult>(protoEvent->prepare_result());
                                    } else {
                                        resultOrError = std::move(error);
                                    }

                                    operation->GetController()->OnPreparationFinished(resultOrError);
                                    break;
                                }
                                case EAgentToSchedulerOperationEventType::MaterializationFinished: {
                                    TErrorOr<TOperationControllerMaterializeResult> resultOrError;
                                    if (error.IsOK()) {
                                        YT_ASSERT(protoEvent->has_materialize_result());
                                        resultOrError = FromProto<TOperationControllerMaterializeResult>(protoEvent->materialize_result());
                                    } else {
                                        resultOrError = std::move(error);
                                    }

                                    operation->GetController()->OnMaterializationFinished(resultOrError);
                                    break;
                                }
                                case EAgentToSchedulerOperationEventType::RevivalFinished: {
                                    TErrorOr<TOperationControllerReviveResult> resultOrError;
                                    if (error.IsOK()) {
                                        YT_ASSERT(protoEvent->has_revive_result());

                                        TOperationControllerReviveResult result;
                                        FromProto(
                                            &result,
                                            protoEvent->revive_result(),
                                            operationId,
                                            agent->GetIncarnationId(),
                                            operation->GetController()->GetPreemptionMode());

                                        resultOrError = std::move(result);
                                    } else {
                                        resultOrError = std::move(error);
                                    }

                                    operation->GetController()->OnRevivalFinished(resultOrError);
                                    break;
                                }
                                case EAgentToSchedulerOperationEventType::CommitFinished: {
                                    TErrorOr<TOperationControllerCommitResult> resultOrError;
                                    if (error.IsOK()) {
                                        YT_ASSERT(protoEvent->has_commit_result());
                                        resultOrError = FromProto<TOperationControllerCommitResult>(protoEvent->commit_result());
                                    } else {
                                        resultOrError = std::move(error);
                                    }

                                    operation->GetController()->OnCommitFinished(resultOrError);
                                    break;
                                }
                                default:
                                    YT_ABORT();
                            }
                        });

                    agent->GetOperationEventsInbox()->ReportStatus(
                        &responseEvents);

                    YT_LOG_DEBUG("Operation events inbox handled");
                });
            }));
    }

    template <class TMethod, class... TArgs>
    void DoRun(IInvokerPtr invoker, TMethod method, TArgs&&... args)
    {
        auto callback = [this, this_ = MakeStrong(this), method] (TArgs&&... args) mutable {
            Bootstrap_->GetScheduler()->ValidateConnected();
            return std::invoke(method, this, std::forward<TArgs>(args)...);
        };

        WaitFor(
            BIND(std::move(callback))
                .AsyncVia(std::move(invoker))
                .Run(std::forward<TArgs>(args)...))
            .ThrowOnError();
    }

    void ProcessAgentHandshake(const TCtxAgentHandshakePtr& context)
    {
        DoRun(
            Bootstrap_->GetControlInvoker(EControlQueue::AgentTracker),
            &TImpl::DoProcessAgentHandshake,
            context);
    }

    void ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
    {
        DoRun(
            GetHeartbeatInvoker(),
            &TImpl::DoProcessAgentHeartbeat,
            context);
    }

    void ProcessAgentScheduleAllocationHeartbeat(const TCtxAgentScheduleAllocationHeartbeatPtr& context)
    {
        DoRun(
            GetHeartbeatInvoker(),
            &TImpl::DoProcessAgentScheduleAllocationHeartbeat,
            context);
    }

    void DoProcessAgentHandshake(const TCtxAgentHandshakePtr& context)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto* request = &context->Request();
        auto* response = &context->Response();

        const auto& agentId = request->agent_id();
        context->SetRequestInfo("AgentId: %v",
            agentId);

        auto existingAgent = FindAgent(agentId);
        if (existingAgent) {
            EControllerAgentState state;
            {
                auto agentGuard = existingAgent->AcquireInnerStateLock();

                state = existingAgent->GetState();
                if (state == EControllerAgentState::Registered || state == EControllerAgentState::WaitingForInitialHeartbeat) {
                    YT_LOG_INFO("Kicking out agent due to id conflict (AgentId: %v, ExistingIncarnationId: %v)",
                        agentId,
                        existingAgent->GetIncarnationId());
                    UnregisterAgent(existingAgent, std::move(agentGuard));
                }
            }

            THROW_ERROR_EXCEPTION(
                "Agent %Qv is in %Qlv state; please retry",
                agentId,
                state);
        }

        auto agent = [&] {
            auto addresses = FromProto<NNodeTrackerClient::TAddressMap>(request->agent_addresses());
            auto tags = FromProto<THashSet<TString>>(request->tags());
            // COMPAT(gritukan): Remove it when controller agents will be fresh enough.
            if (tags.empty()) {
                tags.insert(DefaultOperationTag);
            }

            auto address = NNodeTrackerClient::GetAddressOrThrow(addresses, Bootstrap_->GetLocalNetworks());
            auto channel = Bootstrap_->GetClient()->GetChannelFactory()->CreateChannel(address);

            YT_LOG_INFO("Registering agent (AgentId: %v, Addresses: %v, Tags: %v)",
                agentId,
                addresses,
                tags);

            auto agent = New<TControllerAgent>(
                agentId,
                std::move(addresses),
                std::move(tags),
                std::move(channel),
                Bootstrap_->GetControlInvoker(EControlQueue::AgentTracker),
                GetHeartbeatInvoker(),
                CreateSerializedInvoker(MessageOffloadThreadPool_->GetInvoker(), "controller_agent_tracker"));

            agent->SetState(EControllerAgentState::Registering);
            MutateAgentMappings([agent] (auto& idToAgent) {
                EmplaceOrCrash(idToAgent, agent->GetId(), agent);
            });

            return agent;
        }();

        YT_LOG_INFO(
            "Starting agent incarnation transaction (AgentId: %v)",
            agentId);

        WaitFor(
            BIND(&TImpl::DoRegisterAgent, MakeStrong(this), agent)
                .AsyncVia(GetCancelableControlInvoker())
                .Run())
            .ThrowOnError();

        auto incarnationId = agent->GetIncarnationId();

        ToProto(response->mutable_incarnation_id(), incarnationId);
        response->set_config(ConvertToYsonString(SchedulerConfig_).ToString());
        response->set_scheduler_version(GetVersion());

        context->SetResponseInfo("IncarnationId: %v", incarnationId);
    }

    // TODO(arkady-e1ppa): This method is overly bloated. Split into several methods.
    void DoProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
    {
        VERIFY_INVOKER_AFFINITY(GetHeartbeatInvoker());

        const auto& scheduler = Bootstrap_->GetScheduler();

        auto* request = &context->Request();
        auto* response = &context->Response();

        const auto& agentId = request->agent_id();
        auto incarnationId = FromProto<NControllerAgent::TIncarnationId>(request->incarnation_id());

        context->SetRequestInfo("AgentId: %v, IncarnationId: %v, OperationCount: %v, Memory: %v/%v",
            agentId,
            incarnationId,
            request->operations_size(),
            request->controller_memory_usage(),
            request->controller_memory_limit());

        auto agent = GetMirroredAgentOrThrow(agentId);

        HandleAgentAndRenewLease(agent, incarnationId);

        SwitchTo(agent->GetCancelableHeartbeatInvoker());

        agent->OnHeartbeatReceived();

        ProcessOperationInfos(context, agent);

        auto nodeManager = scheduler->GetNodeManager();

        struct TNodeShardAllocationUpdates
        {
            std::vector<const NProto::TAgentToSchedulerRunningAllocationStatistics*> RunningAllocationStatisticsUpdates;
        };

        auto groupedAllocationUpdates = RunInMessageOffloadInvoker(
            agent,
            [agent, nodeManager, request, response, context, config{Config_}] () {
                const auto Logger = SchedulerLogger
                    .WithTag("RequestId: %v, IncarnationId: %v", context->GetRequestId(), request->agent_id());

                YT_LOG_DEBUG("Group running allocation updates by node shards");

                std::vector<TNodeShardAllocationUpdates> groupedAllocationUpdates(nodeManager->GetNodeShardCount());

                agent->GetRunningAllocationStatisticsUpdatesInbox()->HandleIncoming(
                    request->mutable_agent_to_scheduler_running_allocation_statistics_updates(),
                    [&] (auto* protoStatisticsUpdate) {
                        auto allocationId = FromProto<TAllocationId>(protoStatisticsUpdate->allocation_id());
                        auto shardId = nodeManager->GetNodeShardId(NodeIdFromAllocationId(allocationId));
                        groupedAllocationUpdates[shardId].RunningAllocationStatisticsUpdates.push_back(protoStatisticsUpdate);
                    });

                YT_LOG_DEBUG("Running allocation updates grouped by node shards");

                agent->GetRunningAllocationStatisticsUpdatesInbox()->ReportStatus(
                    response->mutable_agent_to_scheduler_running_allocation_statistics_updates());

                YT_LOG_DEBUG("Handling allocation events outbox");

                agent->GetAbortedAllocationEventsOutbox()->HandleStatus(
                    request->scheduler_to_agent_aborted_allocation_events());
                agent->GetAbortedAllocationEventsOutbox()->BuildOutcoming(
                    response->mutable_scheduler_to_agent_aborted_allocation_events(),
                    config->MaxMessageAllocationEventCount);

                YT_LOG_DEBUG("Allocation events outbox handled");

                YT_LOG_DEBUG("Handling operation events outbox");

                agent->GetOperationEventsOutbox()->HandleStatus(
                    request->scheduler_to_agent_operation_events());
                agent->GetOperationEventsOutbox()->BuildOutcoming(
                    response->mutable_scheduler_to_agent_operation_events(),
                    [] (auto* protoEvent, const auto& event) {
                        protoEvent->set_event_type(static_cast<int>(event.EventType));
                        ToProto(protoEvent->mutable_operation_id(), event.OperationId);
                    });

                YT_LOG_DEBUG("Operation events outbox handled");

                return groupedAllocationUpdates;
            })
            .ValueOrThrow();

        HandleOperationEventsInbox(context, agent);

        if (request->has_controller_memory_limit()) {
            agent->SetMemoryStatistics(TControllerAgentMemoryStatistics{request->controller_memory_limit(), request->controller_memory_usage()});
        }

        if (request->exec_nodes_requested()) {
            RunInMessageOffloadInvoker(agent, [scheduler, context, request, response] {
                    const auto Logger = SchedulerLogger
                        .WithTag("RequestId: %v, IncarnationId: %v", context->GetRequestId(), request->agent_id());
                    YT_LOG_DEBUG("Filling exec node descriptors");
                    response->Attachments().push_back(scheduler->GetCachedProtoExecNodeDescriptors());
                    YT_LOG_DEBUG("Exec node descriptors filled");
                })
                .ThrowOnError();
        }

        RunInMessageOffloadInvoker(agent, [
                context,
                nodeShards = nodeManager->GetNodeShards(),
                nodeShardInvokers = nodeManager->GetNodeShardInvokers(),
                groupedAllocationUpdates = std::move(groupedAllocationUpdates),
                dtorInvoker = MessageOffloadThreadPool_->GetInvoker()
            ] {
                const auto Logger = SchedulerLogger
                    .WithTag("RequestId: %v, IncarnationId: %v", context->GetRequestId(), context->Request().agent_id());

                YT_LOG_DEBUG("Processing allocation events");

                for (int shardId = 0; shardId < std::ssize(nodeShards); ++shardId) {
                    nodeShardInvokers[shardId]->Invoke(
                        BIND([
                            context,
                            nodeShard = nodeShards[shardId],
                            protoUpdates = std::move(groupedAllocationUpdates[shardId]),
                            Logger = SchedulerLogger
                        ] {
                            std::vector<TNodeShard::TRunningAllocationStatisticsUpdate> runningAllocationStatisticsUpdates;
                            runningAllocationStatisticsUpdates.reserve(std::size(protoUpdates.RunningAllocationStatisticsUpdates));
                            for (const auto* protoStatisticsUpdate : protoUpdates.RunningAllocationStatisticsUpdates) {
                                auto allocationId = FromProto<TAllocationId>(protoStatisticsUpdate->allocation_id());

                                auto preemptibleProgressStartTime = NYT::FromProto<TInstant>(protoStatisticsUpdate->preemptible_progress_start_time());

                                runningAllocationStatisticsUpdates.push_back({
                                    .AllocationId = allocationId,
                                    .TimeStatistics = {
                                        .PreemptibleProgressStartTime = preemptibleProgressStartTime,
                                    }});
                            }

                            if (!std::empty(runningAllocationStatisticsUpdates)) {
                                nodeShard->UpdateRunningAllocationsStatistics(runningAllocationStatisticsUpdates);
                            }
                        }));
                }
                YT_LOG_DEBUG("Allocation events are processed");
            })
            .ThrowOnError();

        response->set_operations_archive_version(Bootstrap_->GetScheduler()->GetOperationsArchiveVersion());

        context->SetResponseInfo("IncarnationId: %v", incarnationId);
    }

    void DoProcessAgentScheduleAllocationHeartbeat(const TCtxAgentScheduleAllocationHeartbeatPtr& context)
    {
        VERIFY_INVOKER_AFFINITY(GetHeartbeatInvoker());

        auto* request = &context->Request();
        const auto& agentId = request->agent_id();
        auto incarnationId = FromProto<NControllerAgent::TIncarnationId>(request->incarnation_id());

        context->SetRequestInfo("AgentId: %v, IncarnationId: %v",
            agentId,
            incarnationId);

        auto agent = GetMirroredAgentOrThrow(agentId);

        HandleAgentAndRenewLease(agent, incarnationId);

        SwitchTo(agent->GetCancelableHeartbeatInvoker());

        const auto& nodeManager = Bootstrap_->GetScheduler()->GetNodeManager();
        RunInMessageOffloadInvoker(agent, [
            context,
            agent,
            nodeManager,
            nodeShards = nodeManager->GetNodeShards(),
            nodeShardInvokers = nodeManager->GetNodeShardInvokers(),
            dtorInvoker = MessageOffloadThreadPool_->GetInvoker()
        ] {
                std::vector<std::vector<const NProto::TScheduleAllocationResponse*>> groupedScheduleAllocationResponses(nodeManager->GetNodeShardCount());
                ProcessScheduleAllocationMailboxes(context, agent, nodeManager, groupedScheduleAllocationResponses);
                ProcessScheduleAllocationResponses(
                    context,
                    nodeShards,
                    nodeShardInvokers,
                    std::move(groupedScheduleAllocationResponses),
                    dtorInvoker);
            })
            .ThrowOnError();

        context->SetResponseInfo("IncarnationId: %v", incarnationId);
    }

private:
    TSchedulerConfigPtr SchedulerConfig_;
    TControllerAgentTrackerConfigPtr Config_;
    TAtomicIntrusivePtr<TControllerAgentTrackerConfig> AtomicConfig_;
    TBootstrap* const Bootstrap_;
    const IThreadPoolPtr MessageOffloadThreadPool_;
    const TActionQueuePtr HeartbeatActionQueue_;

    IResponseKeeperPtr ResponseKeeper_;

    THashMap<TAgentId, TControllerAgentPtr> IdToAgent_;
    THashMap<TAgentId, TControllerAgentPtr> MirroredIdToAgent_;

    THashSet<TString> TagsWithTooFewAgents_;
    bool AgentTagsFetched_{};

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    template <class F>
    auto RunInMessageOffloadInvoker(const TControllerAgentPtr& agent, F func) -> TErrorOr<decltype(func())>
    {
        return WaitFor(BIND(func)
            .AsyncVia(agent->GetMessageOffloadInvoker())
            .Run());
    }

    void DoRegisterAgent(TControllerAgentPtr agent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        NApi::TTransactionStartOptions options;
        options.Timeout = Config_->IncarnationTransactionTimeout;
        if (Config_->IncarnationTransactionPingPeriod) {
            options.PingPeriod = Config_->IncarnationTransactionPingPeriod;
        }
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Controller agent incarnation for %v", agent->GetId()));
        options.Attributes = std::move(attributes);
        const auto& lockTransaction = Bootstrap_->GetScheduler()->GetMasterConnector()->GetLockTransaction();
        auto transactionOrError = WaitFor(lockTransaction->StartTransaction(NTransactionClient::ETransactionType::Master, options));

        if (!transactionOrError.IsOK()) {
            Bootstrap_->GetScheduler()->Disconnect(transactionOrError);
            THROW_ERROR_EXCEPTION("Failed to start incarnation transaction") << transactionOrError;
        }

        auto transaction = std::move(transactionOrError.Value());

        {
            auto agentGuard = agent->AcquireInnerStateLock();
            auto agentState = agent->GetState();
            if (agentState != EControllerAgentState::Registering) {
                THROW_ERROR_EXCEPTION(
                    "Failed to complete agent registration (AgentState: %Qlv)",
                    agentState);
            }

            agent->SetIncarnationTransaction(transaction);

            agent->SetLease(TLeaseManager::CreateLease(
                Config_->HeartbeatTimeout,
                BIND_NO_PROPAGATE(&TImpl::OnAgentHeartbeatTimeout, MakeWeak(this), MakeWeak(agent))
                    .Via(GetCancelableControlInvoker())));

            agent->SetState(EControllerAgentState::WaitingForInitialHeartbeat);
        }

        const auto& nodeManager = Bootstrap_->GetScheduler()->GetNodeManager();
        nodeManager->RegisterAgentAtNodeShards(
            agent->GetId(),
            agent->GetAgentAddresses(),
            agent->GetIncarnationId());

        transaction->SubscribeAborted(
            BIND_NO_PROPAGATE(&TImpl::OnAgentIncarnationTransactionAborted, MakeWeak(this), MakeWeak(agent))
                .Via(GetCancelableControlInvoker()));

        YT_LOG_INFO(
            "Agent incarnation transaction started (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());
    }

    void UnregisterAgent(const TControllerAgentPtr& agent, TGuard<NThreading::TSpinLock>&& guard)
    {
        auto agentState = agent->GetState();
        if (agentState == EControllerAgentState::Unregistering ||
            agentState == EControllerAgentState::Unregistered)
        {
            return;
        }

        YT_LOG_INFO("Notify operations that agent is going to unregister (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        YT_VERIFY(agentState == EControllerAgentState::Registered || agentState == EControllerAgentState::WaitingForInitialHeartbeat);

        agent->SetState(EControllerAgentState::Unregistering);

        TerminateAgent(agent, std::move(guard));

        const auto& scheduler = Bootstrap_->GetScheduler();
        for (const auto& operation : agent->Operations()) {
            scheduler->OnOperationAgentUnregistered(operation);
        }

        YT_LOG_INFO("Aborting agent incarnation transaction (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        agent->GetIncarnationTransaction()->Abort()
            .Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                VERIFY_THREAD_AFFINITY(ControlThread);

                // NB: No AgentInnerStateGuard required here since
                // All mutations on agents with state >= Unregistering are done
                // in control thread.
                if (!error.IsOK()) {
                    Bootstrap_->GetScheduler()->Disconnect(error);
                    return;
                }

                if (agent->GetState() != EControllerAgentState::Unregistering) {
                    return;
                }

                YT_LOG_INFO("Agent unregistered (AgentId: %v, IncarnationId: %v)",
                    agent->GetId(),
                    agent->GetIncarnationId());

                agent->SetState(EControllerAgentState::Unregistered);
                MutateAgentMappings([agentId = agent->GetId()] (auto& idToAgent) {
                    EraseOrCrash(idToAgent, agentId);
                });
            })
            .Via(GetCancelableControlInvoker()));

        scheduler->GetNodeManager()->UnregisterAgentFromNodeShards(agent->GetId());
    }

    void TerminateAgent(const TControllerAgentPtr& agent, TGuard<NThreading::TSpinLock>&& guard)
    {
        TLeaseManager::CloseLease(agent->GetLease());
        agent->SetLease(TLease());

        guard.Release();

        TError error("Agent disconnected");
        agent->GetChannel()->Terminate(error);

        agent->Cancel(std::move(error));
    }

    void OnAgentHeartbeatTimeout(const TWeakPtr<TControllerAgent>& weakAgent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = weakAgent.Lock();
        if (!agent) {
            return;
        }

        YT_LOG_WARNING("Agent heartbeat timeout; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        auto agentGuard = agent->AcquireInnerStateLock();
        UnregisterAgent(agent, std::move(agentGuard));
    }

    void OnAgentIncarnationTransactionAborted(const TWeakPtr<TControllerAgent>& weakAgent, const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = weakAgent.Lock();
        if (!agent) {
            return;
        }

        YT_LOG_WARNING(error, "Agent incarnation transaction aborted; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        auto agentGuard = agent->AcquireInnerStateLock();
        UnregisterAgent(agent, std::move(agentGuard));
    }

    void RequestControllerAgentInstances(const NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr& batchReq) const
    {
        YT_LOG_INFO("Requesting controller agents list");

        auto req = TYPathProxy::Get("//sys/controller_agents/instances");
        req->mutable_attributes()->add_keys("tags");
        batchReq->AddRequest(req, "get_agent_list");
    }

    void HandleControllerAgentInstances(const NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_agent_list");
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            EErrorCode::WatcherHandlerFailed,
            "Error getting controller agent list");

        const auto& rsp = rspOrError.Value();

        auto tagToAgentIds = [&] {
            THashMap<TString, std::vector<TString>> tagToAgentIds;

            auto children = ConvertToNode(TYsonString(rsp->value()))->AsMap()->GetChildren();
            for (auto& [agentId, node] : children) {
                const auto tags = [&node{node}, &agentId{agentId}] () -> THashSet<TString> {
                    try {
                        const auto children = node->Attributes().ToMap()->GetChildOrThrow("tags")->AsList()->GetChildren();
                        THashSet<TString> tags;
                        tags.reserve(std::size(children));

                        for (const auto& tagNode : children) {
                            tags.insert(tagNode->AsString()->GetValue());
                        }
                        return tags;
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(ex, "Cannot parse tags of agent %v", agentId);
                        return {};
                    }
                }();

                tagToAgentIds.reserve(std::size(tags));
                for (auto& tag : tags) {
                    tagToAgentIds[std::move(tag)].push_back(agentId);
                }
            }

            return tagToAgentIds;
        }();

        std::vector<TError> errors;
        THashSet<TString> tagsWithTooFewAgents;
        for (const auto& [tag, thresholds] : Config_->TagToAliveControllerAgentThresholds) {
            std::vector<TStringBuf> aliveAgentWithCurrentTag;
            aliveAgentWithCurrentTag.reserve(32);

            for (const auto& [agentId, agent] : IdToAgent_) {
                if (agent->GetTags().contains(tag)) {
                    aliveAgentWithCurrentTag.push_back(agentId);
                }
            }

            const auto agentsWithTag = std::move(tagToAgentIds[tag]);
            const auto agentWithTagCount = std::ssize(agentsWithTag);
            const auto aliveAgentWithTagCount = std::ssize(aliveAgentWithCurrentTag);
            if (aliveAgentWithTagCount < thresholds.Absolute ||
                (agentWithTagCount &&
                    1.0 * aliveAgentWithTagCount / agentWithTagCount < thresholds.Relative)) {

                tagsWithTooFewAgents.insert(tag);
                errors.push_back(
                    TError{"Too few agents matching tag"}
                        << TErrorAttribute{"controller_agent_tag", tag}
                        << TErrorAttribute{"alive_agents", aliveAgentWithCurrentTag}
                        << TErrorAttribute{"agents", agentsWithTag}
                        << TErrorAttribute{"min_alive_agent_count", thresholds.Absolute}
                        << TErrorAttribute{"min_alive_agent_ratio", thresholds.Relative});
            }
        }

        TagsWithTooFewAgents_ = std::move(tagsWithTooFewAgents);
        AgentTagsFetched_ = true;

        TError error;
        if (!errors.empty()) {
            error = TError{EErrorCode::WatcherHandlerFailed, "Too few matching agents"} << std::move(errors);
            YT_LOG_WARNING(error);
        }
        Bootstrap_->GetScheduler()->GetMasterConnector()->SetSchedulerAlert(
            ESchedulerAlertType::TooFewControllerAgentsAlive, error);
    }


    void DoCleanup()
    {
        for (const auto& [agentId, agent] : IdToAgent_) {
            auto agentGuard = agent->AcquireInnerStateLock();
            agent->SetState(EControllerAgentState::Unregistered);
            TerminateAgent(agent, std::move(agentGuard));
        }
        MutateAgentMappings([] (auto& idToAgent) {
            idToAgent.clear();
        });
    }

    void OnMasterConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoCleanup();

        ResponseKeeper_->Start();

        YT_LOG_INFO("Master connected for controller agent tracker");
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ResponseKeeper_->Stop();

        DoCleanup();

        YT_LOG_INFO("Master disconnected for controller agent tracker");
    }

    const IInvokerPtr& GetCancelableControlInvoker()
    {
        return Bootstrap_
            ->GetScheduler()
            ->GetMasterConnector()
            ->GetCancelableControlInvoker(EControlQueue::AgentTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

TControllerAgentTracker::TControllerAgentTracker(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TControllerAgentTracker::~TControllerAgentTracker() = default;

void TControllerAgentTracker::Initialize()
{
    Impl_->Initialize();
}

std::vector<TControllerAgentPtr> TControllerAgentTracker::GetAgents() const
{
    return Impl_->GetAgents();
}

IOperationControllerPtr TControllerAgentTracker::CreateController(const TOperationPtr& operation)
{
    return Impl_->CreateController(operation);
}

TControllerAgentPtr TControllerAgentTracker::PickAgentForOperation(const TOperationPtr& operation)
{
    return Impl_->PickAgentForOperation(operation);
}

void TControllerAgentTracker::AssignOperationToAgent(
    const TOperationPtr& operation,
    const TControllerAgentPtr& agent)
{
    Impl_->AssignOperationToAgent(operation, agent);
}

void TControllerAgentTracker::HandleAgentFailure(
    const TControllerAgentPtr& agent,
    const TError& error)
{
    Impl_->HandleAgentFailure(agent, error);
}

void TControllerAgentTracker::UnregisterOperationFromAgent(const TOperationPtr& operation)
{
    Impl_->UnregisterOperationFromAgent(operation);
}

TControllerAgentTrackerConfigPtr TControllerAgentTracker::GetConfig() const
{
    return Impl_->GetConfig();
}

void TControllerAgentTracker::UpdateConfig(TSchedulerConfigPtr config)
{
    Impl_->UpdateConfig(std::move(config));
}

const IResponseKeeperPtr& TControllerAgentTracker::GetResponseKeeper() const
{
    return Impl_->GetResponseKeeper();
}

IInvokerPtr TControllerAgentTracker::GetHeartbeatInvoker() const
{
    return Impl_->GetHeartbeatInvoker();
}

void TControllerAgentTracker::ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
{
    return Impl_->ProcessAgentHeartbeat(context);
}

void TControllerAgentTracker::ProcessAgentScheduleAllocationHeartbeat(const TCtxAgentScheduleAllocationHeartbeatPtr& context)
{
    return Impl_->ProcessAgentScheduleAllocationHeartbeat(context);
}

void TControllerAgentTracker::ProcessAgentHandshake(const TCtxAgentHandshakePtr& context)
{
    return Impl_->ProcessAgentHandshake(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
