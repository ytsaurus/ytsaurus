#pragma once

#include "public.h"

#include "scheduling_context.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/proto/scheduler_service.pb.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TRecentlyFinishedJobInfo
{
    TOperationId OperationId;
    NProfiling::TCpuInstant EvictionDeadline;
    std::optional<NControllerAgent::TReleaseJobFlags> ReleaseFlags;
};

//! Scheduler-side representation of an execution node.
/*!
 *  Thread affinity: ControlThread (unless noted otherwise)
 */
class TExecNode
    : public TRefCounted
{
private:
    using TJobMap = THashMap<TJobId, TJobPtr>;

public:
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerClient::TNodeId, Id);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::TNodeDescriptor, NodeDescriptor);

    //! Jobs that are currently running on this node.
    DEFINE_BYREF_RW_PROPERTY(THashSet<TJobPtr>, Jobs);

    //! Mapping from job id to job on this node.
    DEFINE_BYREF_RW_PROPERTY(TJobMap, IdToJob);

    //! A set of scheduling tags assigned to this node.
    DEFINE_BYREF_RW_PROPERTY(TBooleanFormulaTags, Tags);

    //! Last time when logging of jobs on node took place.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NProfiling::TCpuInstant>, LastJobsLogTime);

    //! Last time when missing jobs were checked on this node.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NProfiling::TCpuInstant>, LastCheckMissingJobsTime);

    //! Last time when heartbeat from node was processed.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastSeenTime);

    //! Controls node at scheduler.
    DEFINE_BYVAL_RW_PROPERTY(NConcurrency::TLease, RegistrationLease);

    //! Controls heartbeat expiration.
    DEFINE_BYVAL_RW_PROPERTY(NConcurrency::TLease, HeartbeatLease);

    //! State of node at master.
    DEFINE_BYVAL_RW_PROPERTY(NNodeTrackerClient::ENodeState, MasterState);

    //! State of node at scheduler.
    DEFINE_BYVAL_RW_PROPERTY(ENodeState, SchedulerState);

    //! Error of node registration.
    DEFINE_BYVAL_RW_PROPERTY(TError, RegistrationError);

    //! Is |true| iff heartbeat from this node is being processed at the moment.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasOngoingHeartbeat);

    //! Complexity of processing heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(int, SchedulingHeartbeatComplexity);

    //! Stores the time when resources overcommit has detected.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, ResourcesOvercommitStartTime);

    //! Is |true| iff the node must be unregistered but it also has an ongoing
    //! heartbeat so the unregistration has to be postponed until the heartbeat processing
    //! is complete.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasPendingUnregistration);

    //! Has value iff the node jobs must be aborted but node also has an ongoing
    //! heartbeat so jobs abortion has to be postponed until the heartbeat processing
    //! is complete.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<EAbortReason>, PendingJobsAbortionReason);

    //! Jobs at this node that are waiting for confirmation.
    DEFINE_BYREF_RW_PROPERTY(THashSet<TJobId>, UnconfirmedJobIds);

    //! Ids of jobs that have been finished recently and are yet to be saved to the snapshot.
    //! We remember them in order to not remove them as unknown jobs.
    //! We store an eviction deadline for every job id to make sure
    //! that no job is stored infinitely.
    using TRecentlyFinishedJobIdToInfo = THashMap<TJobId, TRecentlyFinishedJobInfo>;
    DEFINE_BYREF_RW_PROPERTY(TRecentlyFinishedJobIdToInfo, RecentlyFinishedJobs);

    DEFINE_BYVAL_RO_PROPERTY(double, IOWeight);

    //! Mark that node has large job archivation queues.
    DEFINE_BYVAL_RW_PROPERTY(bool, JobReporterQueueIsTooLarge);

    DEFINE_BYVAL_RW_PROPERTY(TScheduleJobsStatistics, LastPreemptiveHeartbeatStatistics);
    DEFINE_BYVAL_RW_PROPERTY(TScheduleJobsStatistics, LastNonPreemptiveHeartbeatStatistics);

    DEFINE_BYVAL_RW_PROPERTY(std::optional<TString>, InfinibandCluster);

    DEFINE_BYVAL_RW_PROPERTY(NYTree::IAttributeDictionaryPtr, SchedulingOptions);

public:
    TExecNode(
        NNodeTrackerClient::TNodeId id,
        NNodeTrackerClient::TNodeDescriptor nodeDescriptor,
        ENodeState state);

    const TString& GetDefaultAddress() const;

    //! Checks if the node can handle jobs demanding a certain #tag.
    bool CanSchedule(const TSchedulingTagFilter& filter) const;

    //! Constructs a descriptor containing the current snapshot of node's state.
    /*!
     *  Thread affinity: any
     */
    TExecNodeDescriptor BuildExecDescriptor() const;

    //! Set the node's IO weight.
    void SetIOWeights(const THashMap<TString, double>& mediumToWeight);

    //! Returns the node's resource limits, as reported by the node.
    const TJobResources& GetResourceLimits() const;

    //! Sets the node's resource limits.
    void SetResourceLimits(const TJobResources& value);

    //! Returns the most recent resource usage, as reported by the node.
    /*!
     *  Some fields are also updated by the scheduler strategy to
     *  reflect recent job set changes.
     *  E.g. when the scheduler decides to
     *  start a new job it decrements the appropriate counters.
     */
    const TJobResources& GetResourceUsage() const;

    const NNodeTrackerClient::NProto::TDiskResources& GetDiskResources() const;

    //! Sets the node's resource usage.
    void SetResourceUsage(const TJobResources& value);

    void SetDiskResources(const NNodeTrackerClient::NProto::TDiskResources& value);

    void BuildAttributes(NYTree::TFluentMap fluent);

private:
    TJobResources ResourceUsage_;
    NNodeTrackerClient::NProto::TDiskResources DiskResources_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    TJobResources ResourceLimits_;
};

DEFINE_REFCOUNTED_TYPE(TExecNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
