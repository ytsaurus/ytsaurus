#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/message_queue.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/ytlib/scheduler/job_resources.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerToAgentJobEvent
{
    ESchedulerToAgentJobEventType EventType;
    TOperationId OperationId;
    bool LogAndProfile;
    TInstant StartTime;
    std::optional<TInstant> FinishTime;
    std::unique_ptr<NJobTrackerClient::NProto::TJobStatus> Status;
    std::optional<EAbortReason> AbortReason;
    std::optional<bool> Abandoned;
    std::optional<EInterruptReason> InterruptReason;
    std::optional<bool> AbortedByScheduler;
    std::optional<TPreemptedFor> PreemptedFor;
};

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
    NNodeTrackerClient::TNodeId NodeId;
    TJobResources NodeResourceLimits;
    NNodeTrackerClient::NProto::TDiskResources NodeDiskResources;
    TScheduleJobSpec Spec;
};

using TScheduleJobRequestPtr = std::unique_ptr<TScheduleJobRequest>;

void ToProto(NProto::TScheduleJobRequest* protoRequest, const TScheduleJobRequest& request);

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
        const IInvokerPtr& invoker);

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

    const NApi::ITransactionPtr& GetIncarnationTransaction() const;
    void SetIncarnationTransaction(NApi::ITransactionPtr transaction);

    TMessageQueueInbox* GetOperationEventsInbox();
    TMessageQueueInbox* GetJobEventsInbox();
    TMessageQueueInbox* GetScheduleJobResponsesInbox();

    const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>>& GetJobEventsOutbox();
    const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>& GetOperationEventsOutbox();
    const TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>>& GetScheduleJobRequestsOutbox();

    void Cancel(const TError& error);
    const IInvokerPtr& GetCancelableInvoker();

    std::optional<TControllerAgentMemoryStatistics> GetMemoryStatistics();
    void SetMemoryStatistics(TControllerAgentMemoryStatistics memoryStatistics);

private:
    const TAgentId Id_;
    const NNodeTrackerClient::TAddressMap AgentAddresses_;
    const THashSet<TString> Tags_;
    const NRpc::IChannelPtr Channel_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    NApi::ITransactionPtr IncarnationTransaction_;

    std::unique_ptr<TMessageQueueInbox> OperationEventsInbox_;
    std::unique_ptr<TMessageQueueInbox> JobEventsInbox_;
    std::unique_ptr<TMessageQueueInbox> ScheduleJobResponsesInbox_;

    TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>> JobEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>> OperationEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>> ScheduleJobRequestsOutbox_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, MemoryStatisticsLock_);
    std::optional<TControllerAgentMemoryStatistics> MemoryStatistics_;
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
