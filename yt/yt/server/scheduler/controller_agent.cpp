#include "controller_agent.h"

#include "private.h"

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/actions/cancelable_context.h>

namespace NYT::NScheduler {

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TScheduleJobRequest* protoRequest, const TScheduleJobRequest& request)
{
    ToProto(protoRequest->mutable_operation_id(), request.OperationId);
    ToProto(protoRequest->mutable_job_id(), request.JobId);
    protoRequest->set_tree_id(request.TreeId);
    ToProto(protoRequest->mutable_job_resource_limits(), request.JobResourceLimits);
    protoRequest->set_pool_path(request.PoolPath);
    ToProto(protoRequest->mutable_node_disk_resources(), request.NodeDiskResources);
    auto* spec = protoRequest->mutable_spec();
    if (request.Spec.WaitingJobTimeout) {
        spec->set_waiting_job_timeout(ToProto<i64>(*request.Spec.WaitingJobTimeout));
    }
}

////////////////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent(
    const TString& id,
    const NNodeTrackerClient::TAddressMap& agentAddresses,
    THashSet<TString> tags,
    NRpc::IChannelPtr channel,
    const IInvokerPtr& invoker,
    const IInvokerPtr& messageOffloadInvoker)
    : Id_(id)
    , AgentAddresses_(agentAddresses)
    , Tags_(std::move(tags))
    , Channel_(std::move(channel))
    , CancelableContext_(New<TCancelableContext>())
    , CancelableInvoker_(CancelableContext_->CreateInvoker(invoker))
    , MessageOffloadInvoker_(messageOffloadInvoker)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
}

const TAgentId& TControllerAgent::GetId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Id_;
}

const NNodeTrackerClient::TAddressMap& TControllerAgent::GetAgentAddresses() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return AgentAddresses_;
}

const THashSet<TString>& TControllerAgent::GetTags() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Tags_;
}

const NRpc::IChannelPtr& TControllerAgent::GetChannel() const
{
    return Channel_;
}

TIncarnationId TControllerAgent::GetIncarnationId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return NControllerAgent::IncarnationIdFromTransactionId(IncarnationTransaction_->GetId());
}

IInvokerPtr TControllerAgent::GetMessageOffloadInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return MessageOffloadInvoker_;
}

const NApi::ITransactionPtr& TControllerAgent::GetIncarnationTransaction() const
{
    return IncarnationTransaction_;
}

void TControllerAgent::SetIncarnationTransaction(NApi::ITransactionPtr transaction)
{
    YT_VERIFY(!IncarnationTransaction_);
    IncarnationTransaction_ = std::move(transaction);

    OperationEventsInbox_ = std::make_unique<TMessageQueueInbox>(
        SchedulerLogger.WithTag("Kind: AgentToSchedulerOperations, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()),
        SchedulerProfiler.WithTag("queue", "operation_events"),
        CancelableInvoker_);
    RunningJobStatisticsUpdatesInbox_ = std::make_unique<TMessageQueueInbox>(
        SchedulerLogger.WithTag("Kind: AgentToSchedulerRunningJobStatisticsUpdates, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()),
        SchedulerProfiler.WithTag("queue", "running_job_statistics_updates"),
        MessageOffloadInvoker_);
    ScheduleJobResponsesInbox_ = std::make_unique<TMessageQueueInbox>(
        SchedulerLogger.WithTag("Kind: AgentToSchedulerScheduleJobResponses, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()),
        SchedulerProfiler.WithTag("queue", "schedule_job_responses"),
        MessageOffloadInvoker_);
    AbortedAllocationEventsOutbox_ = New<TMessageQueueOutbox<TAbortedAllocationSummary>>(
        SchedulerLogger.WithTag("Kind: SchedulerToAgentJobs, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()),
        SchedulerProfiler.WithTag("queue", "job_events"),
        MessageOffloadInvoker_);

    OperationEventsOutbox_ = New<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>(
        SchedulerLogger.WithTag("Kind: SchedulerToAgentOperations, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()),
        SchedulerProfiler.WithTag("queue", "operation_events"),
        MessageOffloadInvoker_);
    ScheduleJobRequestsOutbox_ = New<TMessageQueueOutbox<TScheduleJobRequestPtr>>(
        SchedulerLogger.WithTag("Kind: SchedulerToAgentScheduleJobRequests, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()),
        SchedulerProfiler.WithTag("queue", "schedule_job_requests"),
        MessageOffloadInvoker_,
        /*supportTracing*/ true);
}

TMessageQueueInbox* TControllerAgent::GetOperationEventsInbox()
{
    return OperationEventsInbox_.get();
}

TMessageQueueInbox* TControllerAgent::GetRunningJobStatisticsUpdatesInbox()
{
    return RunningJobStatisticsUpdatesInbox_.get();
}

TMessageQueueInbox* TControllerAgent::GetScheduleJobResponsesInbox()
{
    return ScheduleJobResponsesInbox_.get();
}

const TSchedulerToAgentAbortedAllocationEventOutboxPtr& TControllerAgent::GetAbortedAllocationEventsOutbox()
{
    return AbortedAllocationEventsOutbox_;
}

const TSchedulerToAgentOperationEventOutboxPtr& TControllerAgent::GetOperationEventsOutbox()
{
    return OperationEventsOutbox_;
}

const TScheduleJobRequestOutboxPtr& TControllerAgent::GetScheduleJobRequestsOutbox()
{
    return ScheduleJobRequestsOutbox_;
}

void TControllerAgent::Cancel(const TError& error)
{
    CancelableContext_->Cancel(error);

    MaybeError_ = error;

    for (const auto& [_, promise] : CounterToFullHeartbeatProcessedPromise_) {
        promise.TrySet(error);
    }
}

const IInvokerPtr& TControllerAgent::GetCancelableInvoker()
{
    return CancelableInvoker_;
}

std::optional<TControllerAgentMemoryStatistics> TControllerAgent::GetMemoryStatistics()
{
    auto guard = Guard(MemoryStatisticsLock_);

    return MemoryStatistics_;
}

void TControllerAgent::SetMemoryStatistics(TControllerAgentMemoryStatistics memoryStatistics)
{
    auto guard = Guard(MemoryStatisticsLock_);

    MemoryStatistics_ = memoryStatistics;
}

TFuture<void> TControllerAgent::GetFullHeartbeatProcessed()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (MaybeError_) {
        return MakeFuture(*MaybeError_);
    }

    auto it = CounterToFullHeartbeatProcessedPromise_.find(HeartbeatCounter_ + 2);
    if (it == CounterToFullHeartbeatProcessedPromise_.end()) {
        // At least one full heartbeat should be processed since current one.
        it = CounterToFullHeartbeatProcessedPromise_.emplace(HeartbeatCounter_ + 2, NewPromise<void>()).first;
    }
    return it->second;
}

void TControllerAgent::OnHeartbeatReceived()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ++HeartbeatCounter_;

    while (true) {
        auto it = CounterToFullHeartbeatProcessedPromise_.begin();
        if (it == CounterToFullHeartbeatProcessedPromise_.end()) {
            break;
        }

        if (it->first < HeartbeatCounter_) {
            YT_VERIFY(it->second.IsSet());
            CounterToFullHeartbeatProcessedPromise_.erase(it);
        } else if (it->first == HeartbeatCounter_) {
            it->second.TrySet();
            break;
        } else {
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
