#pragma once

#include "public.h"
#include "message_queue.h"

#include <yt/server/controller_agent/public.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/rpc/public.h>

#include <yt/core/misc/property.h>

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

//! Scheduler-side representation of a controller agent.
/*!
 *  Thread affinity: Control thread (unless noted otherwise)
 */
class TControllerAgent
    : public TIntrinsicRefCounted
{
public:
    explicit TControllerAgent(const NControllerAgent::TIncarnationId& incarnationId);

    DEFINE_BYVAL_RO_PROPERTY(NControllerAgent::TIncarnationId, IncarnationId);

    DEFINE_BYVAL_RW_PROPERTY(NYson::TYsonString, SuspiciousJobsYson);

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMessageQueueInbox, OperationEventsInbox);
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMessageQueueInbox, JobEventsInbox);
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMessageQueueInbox, ScheduleJobResponsesInbox);

    DEFINE_BYVAL_RO_PROPERTY(TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>>, JobEventsOutbox);
    DEFINE_BYVAL_RO_PROPERTY(TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>>, OperationEventsOutbox);
    DEFINE_BYVAL_RO_PROPERTY(TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>>, ScheduleJobRequestsOutbox);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgent)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
