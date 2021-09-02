#include "controller_agent.h"
#include "private.h"

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
    // COMPAT (remove after CA update)
    ToProto(protoRequest->mutable_node_resource_limits(), request.NodeResourceLimits);
    protoRequest->mutable_node_disk_resources()->CopyFrom(request.NodeDiskResources);
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
    const IInvokerPtr& invoker)
    : Id_(id)
    , AgentAddresses_(agentAddresses)
    , Tags_(std::move(tags))
    , Channel_(std::move(channel))
    , CancelableContext_(New<TCancelableContext>())
    , CancelableInvoker_(CancelableContext_->CreateInvoker(invoker))
{ }

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

    return IncarnationTransaction_->GetId();
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
            GetIncarnationId()));
    JobEventsInbox_ = std::make_unique<TMessageQueueInbox>(
        SchedulerLogger.WithTag("Kind: AgentToSchedulerJobs, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()));
    ScheduleJobResponsesInbox_ = std::make_unique<TMessageQueueInbox>(
        SchedulerLogger.WithTag("Kind: AgentToSchedulerScheduleJobResponses, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()));
    JobEventsOutbox_ = New<TMessageQueueOutbox<TSchedulerToAgentJobEvent>>(
        SchedulerLogger.WithTag("Kind: SchedulerToAgentJobs, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()));
    OperationEventsOutbox_ = New<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>(
        SchedulerLogger.WithTag("Kind: SchedulerToAgentOperations, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()));
    ScheduleJobRequestsOutbox_ = New<TMessageQueueOutbox<TScheduleJobRequestPtr>>(
        SchedulerLogger.WithTag("Kind: SchedulerToAgentScheduleJobRequests, AgentId: %v, IncarnationId: %v",
            Id_,
            GetIncarnationId()));
}

TMessageQueueInbox* TControllerAgent::GetOperationEventsInbox()
{
    return OperationEventsInbox_.get();
}

TMessageQueueInbox* TControllerAgent::GetJobEventsInbox()
{
    return JobEventsInbox_.get();
}

TMessageQueueInbox* TControllerAgent::GetScheduleJobResponsesInbox()
{
    return ScheduleJobResponsesInbox_.get();
}

const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>>& TControllerAgent::GetJobEventsOutbox()
{
    return JobEventsOutbox_;
}

const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>& TControllerAgent::GetOperationEventsOutbox()
{
    return OperationEventsOutbox_;
}

const TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>>& TControllerAgent::GetScheduleJobRequestsOutbox()
{
    return ScheduleJobRequestsOutbox_;
}

void TControllerAgent::Cancel(const TError& error)
{
    CancelableContext_->Cancel(error);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
