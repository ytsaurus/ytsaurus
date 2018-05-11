#pragma once

#include "public.h"
#include "message_queue.h"

#include <yt/server/controller_agent/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/rpc/public.h>

#include <yt/core/misc/property.h>

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerToAgentJobEvent
{
    ESchedulerToAgentJobEventType EventType;
    TOperationId OperationId;
    bool LogAndProfile;
    TInstant StartTime;
    TNullable<TInstant> FinishTime;
    std::unique_ptr<NJobTrackerClient::NProto::TJobStatus> Status;
    TNullable<EAbortReason> AbortReason;
    TNullable<bool> Abandoned;
    TNullable<EInterruptReason> InterruptReason;
    TNullable<bool> AbortedByScheduler;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerToAgentOperationEvent
{
    ESchedulerToAgentOperationEventType EventType;
    TOperationId OperationId;
};

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobRequest
{
    TOperationId OperationId;
    TJobId JobId;
    NScheduler::TJobResourcesWithQuota JobResourceLimits;
    TString TreeId;
    NNodeTrackerClient::TNodeId NodeId;
    TJobResources NodeResourceLimits;
    NNodeTrackerClient::NProto::TDiskResources NodeDiskInfo;
};

using TScheduleJobRequestPtr = std::unique_ptr<TScheduleJobRequest>;

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
    /*
     * \note Thread affinity: any
     */
    const NRpc::IChannelPtr& GetChannel() const;
    /*
     * \note Thread affinity: any
     */
    const TIncarnationId& GetIncarnationId() const;

    const NApi::ITransactionPtr& GetIncarnationTransaction() const;
    void SetIncarnationTransaction(NApi::ITransactionPtr transaction);

    TMessageQueueInbox* GetOperationEventsInbox();
    TMessageQueueInbox* GetJobEventsInbox();
    TMessageQueueInbox* GetScheduleJobResponsesInbox();

    const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>>& GetJobEventsOutbox();
    const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>& GetOperationEventsOutbox();
    const TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>>& GetScheduleJobRequestsOutbox();

    void Cancel();
    const IInvokerPtr& GetCancelableInvoker();

private:
    const TAgentId Id_;
    const NNodeTrackerClient::TAddressMap AgentAddresses_;
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
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
