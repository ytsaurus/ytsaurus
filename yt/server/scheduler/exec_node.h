#pragma once

#include "public.h"

#include "scheduling_context.h"

#include <yt/server/lib/controller_agent/helpers.h>

#include <yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/proto/scheduler_service.pb.h>
#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/lease_manager.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/property.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TRecentlyFinishedJobInfo
{
    TOperationId OperationId;
    NProfiling::TCpuInstant EvictionDeadline;
    std::optional<NJobTrackerClient::TReleaseJobFlags> ReleaseFlags;
};

//! Scheduler-side representation of an execution node.
/*!
 *  Thread affinity: ControlThread (unless noted otherwise)
 */
class TExecNode
    : public TIntrinsicRefCounted
{
private:
    typedef THashMap<TJobId, TJobPtr> TJobMap;

public:
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerClient::TNodeId, Id);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::TNodeDescriptor, NodeDescriptor);

    //! Jobs that are currently running on this node.
    DEFINE_BYREF_RW_PROPERTY(THashSet<TJobPtr>, Jobs);

    //! Mapping from job id to job on this node.
    DEFINE_BYREF_RW_PROPERTY(TJobMap, IdToJob);

    //! A set of scheduling tags assigned to this node.
    DEFINE_BYREF_RW_PROPERTY(THashSet<TString>, Tags);

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

    //! Stores the time when resources overcommit has detected.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, ResourcesOvercommitStartTime);

    //! Is |true| iff the node must be unregistered but it also has an ongoing
    //! heartbeat so the unregistration has to be postponed until the heartbeat processing
    //! is complete.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasPendingUnregistration);

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

    DEFINE_BYVAL_RW_PROPERTY(TFairShareSchedulingStatistics, LastHeartbeatStatistics);

public:
    TExecNode(
        NNodeTrackerClient::TNodeId id,
        const NNodeTrackerClient::TNodeDescriptor& nodeDescriptor,
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

    mutable NConcurrency::TReaderWriterSpinLock SpinLock_;
    TJobResources ResourceLimits_;
};

DEFINE_REFCOUNTED_TYPE(TExecNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
