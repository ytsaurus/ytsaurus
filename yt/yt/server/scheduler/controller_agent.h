#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/message_queue.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/server/lib/controller_agent/public.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/scheduler/disk_resources.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using TAbortedAllocationSummary = NControllerAgent::TAbortedAllocationSummary;

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerToAgentOperationEvent
{
    ESchedulerToAgentOperationEventType EventType;
    TOperationId OperationId;
};

////////////////////////////////////////////////////////////////////////////////

struct TScheduleAllocationSpec
{
    std::optional<TDuration> WaitingForResourcesOnNodeTimeout;
};

struct TScheduleAllocationRequest
{
    TOperationId OperationId;
    TAllocationId AllocationId;
    TJobResources AllocationResourceLimits;
    TString TreeId;
    TString PoolPath;
    NNodeTrackerClient::TNodeId NodeId;
    TJobResources NodeResourceLimits;
    TDiskResources NodeDiskResources;
    TScheduleAllocationSpec Spec;
};

using TScheduleAllocationRequestPtr = std::unique_ptr<TScheduleAllocationRequest>;

void ToProto(NProto::TScheduleAllocationRequest* protoRequest, const TScheduleAllocationRequest& request);

////////////////////////////////////////////////////////////////////////////////

using TSchedulerToAgentAbortedAllocationEventOutboxPtr = TIntrusivePtr<TMessageQueueOutbox<TAbortedAllocationSummary>>;
using TSchedulerToAgentOperationEventOutboxPtr = TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>;
using TScheduleAllocationRequestOutboxPtr = TIntrusivePtr<TMessageQueueOutbox<TScheduleAllocationRequestPtr>>;

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
        const IInvokerPtr& heartbeatInvoker,
        const IInvokerPtr& messageOffloadInvoker);

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

    /*
     * \note Thread affinity: any
     */
    TGuard<NThreading::TSpinLock> AcquireInnerStateLock();

    /*
     * \note Thread affinity: any
     */
    void SetState(EControllerAgentState newState);

    /*
     * \note Thread affinity: any
     */
    EControllerAgentState GetState() const;

    const NApi::ITransactionPtr& GetIncarnationTransaction() const;
    void SetIncarnationTransaction(NApi::ITransactionPtr transaction);

    TMessageQueueInbox* GetOperationEventsInbox();
    TMessageQueueInbox* GetRunningAllocationStatisticsUpdatesInbox();
    TMessageQueueInbox* GetScheduleAllocationResponsesInbox();

    const TSchedulerToAgentAbortedAllocationEventOutboxPtr& GetAbortedAllocationEventsOutbox();
    const TSchedulerToAgentOperationEventOutboxPtr& GetOperationEventsOutbox();
    const TScheduleAllocationRequestOutboxPtr& GetScheduleAllocationRequestsOutbox();

    void Cancel(const TError& error);

    /*
     * \note Thread affinity: any
     */
    const IInvokerPtr& GetCancelableControlInvoker();

    /*
     * \note Thread affinity: any
     */
    const IInvokerPtr& GetCancelableHeartbeatInvoker();

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

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, InnerStateLock_);

    std::atomic<EControllerAgentState> State_;

    TCancelableContextPtr CancelableContext_;
    const IInvokerPtr CancelableControlInvoker_;

    const IInvokerPtr HeartbeatInvoker_;
    const IInvokerPtr CancelableHeartbeatInvoker_;

    IInvokerPtr MessageOffloadInvoker_;

    NApi::ITransactionPtr IncarnationTransaction_;

    std::unique_ptr<TMessageQueueInbox> OperationEventsInbox_;
    std::unique_ptr<TMessageQueueInbox> RunningAllocationStatisticsUpdatesInbox_;
    std::unique_ptr<TMessageQueueInbox> ScheduleAllocationResponsesInbox_;

    TSchedulerToAgentAbortedAllocationEventOutboxPtr AbortedAllocationEventsOutbox_;
    TSchedulerToAgentOperationEventOutboxPtr OperationEventsOutbox_;
    TScheduleAllocationRequestOutboxPtr ScheduleAllocationRequestsOutbox_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, MemoryStatisticsLock_);
    std::optional<TControllerAgentMemoryStatistics> MemoryStatistics_;

    std::optional<TError> MaybeError_;

    i64 HeartbeatCounter_ = 0;
    std::map<i64, TPromise<void>> CounterToFullHeartbeatProcessedPromise_;
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
