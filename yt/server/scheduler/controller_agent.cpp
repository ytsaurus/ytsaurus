#include "controller_agent.h"
#include "private.h"

#include <yt/client/api/transaction.h>

#include <yt/core/actions/cancelable_context.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TControllerAgent::TControllerAgent(
    const TString& id,
    const NNodeTrackerClient::TAddressMap& agentAddresses,
    NRpc::IChannelPtr channel,
    const IInvokerPtr& invoker)
    : Id_(id)
    , AgentAddresses_(agentAddresses)
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

const NRpc::IChannelPtr& TControllerAgent::GetChannel() const
{
    return Channel_;
}

const TIncarnationId& TControllerAgent::GetIncarnationId() const
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
    YCHECK(!IncarnationTransaction_);
    IncarnationTransaction_ = std::move(transaction);

    OperationEventsInbox_ = std::make_unique<TMessageQueueInbox>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: AgentToSchedulerOperations, AgentId: %v, IncarnationId: %v",
                Id_,
                GetIncarnationId()));
    JobEventsInbox_ = std::make_unique<TMessageQueueInbox>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: AgentToSchedulerJobs, AgentId: %v, IncarnationId: %v",
                Id_,
                GetIncarnationId()));
    ScheduleJobResponsesInbox_ = std::make_unique<TMessageQueueInbox>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: AgentToSchedulerScheduleJobResponses, AgentId: %v, IncarnationId: %v",
                Id_,
                GetIncarnationId()));
    JobEventsOutbox_ = New<TMessageQueueOutbox<TSchedulerToAgentJobEvent>>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: SchedulerToAgentJobs, AgentId: %v, IncarnationId: %v",
                Id_,
                GetIncarnationId()));
    OperationEventsOutbox_ = New<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: SchedulerToAgentOperations, AgentId: %v, IncarnationId: %v",
                Id_,
                GetIncarnationId()));
    ScheduleJobRequestsOutbox_ = New<TMessageQueueOutbox<TScheduleJobRequestPtr>>(
        NLogging::TLogger(SchedulerLogger)
            .AddTag("Kind: SchedulerToAgentScheduleJobRequests, AgentId: %v, IncarnationId: %v",
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

void TControllerAgent::Cancel()
{
    CancelableContext_->Cancel();
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
