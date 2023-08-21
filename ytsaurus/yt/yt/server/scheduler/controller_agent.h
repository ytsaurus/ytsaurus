#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/message_queue.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/server/lib/controller_agent/public.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TStartedJobSummary = NControllerAgent::TStartedJobSummary;
using TFinishedJobSummary = NControllerAgent::TFinishedJobSummary;
using TAbortedBySchedulerJobSummary = NControllerAgent::TAbortedBySchedulerJobSummary;

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerToAgentOperationEvent
{
    ESchedulerToAgentOperationEventType EventType;
    TOperationId OperationId;
};

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobSpec
{
    std::optional<TDuration> WaitingJobTimeout;
};

struct TScheduleJobRequest
{
    TOperationId OperationId;
    TJobId JobId;
    TJobResources JobResourceLimits;
    TString TreeId;
    TString PoolPath;
    NNodeTrackerClient::TNodeId NodeId;
    TJobResources NodeResourceLimits;
    NNodeTrackerClient::NProto::TDiskResources NodeDiskResources;
    TScheduleJobSpec Spec;
};

using TScheduleJobRequestPtr = std::unique_ptr<TScheduleJobRequest>;

void ToProto(NProto::TScheduleJobRequest* protoRequest, const TScheduleJobRequest& request);

////////////////////////////////////////////////////////////////////////////////

using TSchedulerToAgentJobEventOutboxPtr = TIntrusivePtr<TMessageQueueOutbox<TAbortedBySchedulerJobSummary>>;
using TSchedulerToAgentOperationEventOutboxPtr = TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>;
using TScheduleJobRequestOutboxPtr = TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>>;

////////////////////////////////////////////////////////////////////////////////


struct TControllerAgentMemoryStatistics
{
    i64 Limit;
    i64 Usage;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControllerAgentState,
    (Registering)
    (Registered)
    (Unregistering)
    (Unregistered)
    (WaitingForInitialHeartbeat)
);

//! Scheduler-side representation of a controller agent.
/*!
 *  Thread affinity: Control thread (unless noted otherwise)
 */
class TControllerAgent
    : public TRefCounted
{
public:
    TControllerAgent(
        const TAgentId& id,
        const NNodeTrackerClient::TAddressMap& agentAddresses,
        THashSet<TString> tags,
        NRpc::IChannelPtr channel,
        const IInvokerPtr& invoker,
        const IInvokerPtr& messageOffloadInvoker);

    DEFINE_BYVAL_RW_PROPERTY(EControllerAgentState, State);
    DEFINE_BYVAL_RW_PROPERTY(NConcurrency::TLease, Lease);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TOperationPtr>, Operations);

public:
    /*
     * \note Thread affinity: any
     */
    const TAgentId& GetId() const;
    /*
     * \note Thread affinity: any
     */
    const NNodeTrackerClient::TAddressMap& GetAgentAddresses() const;

    //! Returns the set of tags assigned to controller agent.
    /*
     * \note Thread affinity: any
     */
    const THashSet<TString>& GetTags() const;
    /*
     * \note Thread affinity: any
     */
    const NRpc::IChannelPtr& GetChannel() const;
    /*
     * \note Thread affinity: any
     */
    TIncarnationId GetIncarnationId() const;

    /*
     * \note Thread affinity: any
     */
    IInvokerPtr GetMessageOffloadInvoker() const;

    const NApi::ITransactionPtr& GetIncarnationTransaction() const;
    void SetIncarnationTransaction(NApi::ITransactionPtr transaction);

    TMessageQueueInbox* GetOperationEventsInbox();
    TMessageQueueInbox* GetRunningJobStatisticsUpdatesInbox();
    TMessageQueueInbox* GetScheduleJobResponsesInbox();

    const TSchedulerToAgentJobEventOutboxPtr& GetJobEventsOutbox();
    const TSchedulerToAgentOperationEventOutboxPtr& GetOperationEventsOutbox();
    const TScheduleJobRequestOutboxPtr& GetScheduleJobRequestsOutbox();

    void Cancel(const TError& error);
    const IInvokerPtr& GetCancelableInvoker();

    std::optional<TControllerAgentMemoryStatistics> GetMemoryStatistics();
    void SetMemoryStatistics(TControllerAgentMemoryStatistics memoryStatistics);

    TFuture<void> GetFullHeartbeatProcessed();
    void OnHeartbeatReceived();

private:
    const TAgentId Id_;
    const NNodeTrackerClient::TAddressMap AgentAddresses_;
    const THashSet<TString> Tags_;
    const NRpc::IChannelPtr Channel_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    IInvokerPtr MessageOffloadInvoker_;

    NApi::ITransactionPtr IncarnationTransaction_;

    std::unique_ptr<TMessageQueueInbox> OperationEventsInbox_;
    std::unique_ptr<TMessageQueueInbox> RunningJobStatisticsUpdatesInbox_;
    std::unique_ptr<TMessageQueueInbox> ScheduleJobResponsesInbox_;

    TSchedulerToAgentJobEventOutboxPtr JobEventsOutbox_;
    TSchedulerToAgentOperationEventOutboxPtr OperationEventsOutbox_;
    TScheduleJobRequestOutboxPtr ScheduleJobRequestsOutbox_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, MemoryStatisticsLock_);
    std::optional<TControllerAgentMemoryStatistics> MemoryStatistics_;

    std::optional<TError> MaybeError_;

    i64 HeartbeatCounter_ = 0;
    std::map<i64, TPromise<void>> CounterToFullHeartbeatProcessedPromise_;
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
