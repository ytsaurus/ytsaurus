#pragma once

#include "public.h"
#include "scheduling_tag.h"

#include <yt/server/node_tracker_server/node.h>

#include <yt/server/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/client/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/proto/scheduler_service.pb.h>
#include <yt/ytlib/scheduler/job.h>
#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/lease_manager.h>
#include <yt/core/misc/property.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TRecentlyFinishedJobInfo
{
    TOperationId OperationId;
    NProfiling::TCpuInstant EvictionDeadline;
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
    DEFINE_BYVAL_RW_PROPERTY(TNullable<NProfiling::TCpuInstant>, LastJobsLogTime);

    //! Last time when missing jobs were checked on this node.
    DEFINE_BYVAL_RW_PROPERTY(TNullable<NProfiling::TCpuInstant>, LastCheckMissingJobsTime);

    //! Last time when heartbeat from node was processed.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastSeenTime);

    //! Controls heartbeat expiration.
    DEFINE_BYVAL_RW_PROPERTY(NConcurrency::TLease, Lease);

    //! State of node at master.
    DEFINE_BYVAL_RW_PROPERTY(NNodeTrackerServer::ENodeState, MasterState);

    //! Error of node registration.
    DEFINE_BYVAL_RW_PROPERTY(TError, RegistrationError);

    //! Is |true| iff heartbeat from this node is being processed at the moment.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasOngoingHeartbeat);

    //! Is |true| iff jobs are scheduled on the node at the moment by the strategy.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasOngoingJobsScheduling);

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

    //! Jobs that are to be removed with a next heartbeat response.
    DEFINE_BYREF_RW_PROPERTY(std::vector<TJobToRelease>, JobsToRemove);

    DEFINE_BYVAL_RO_PROPERTY(double, IOWeight);
    
    //! Mark that node has large job archivation queues.
    DEFINE_BYVAL_RW_PROPERTY(bool, JobReporterQueueIsTooLarge);

public:
    TExecNode(
        NNodeTrackerClient::TNodeId id,
        const NNodeTrackerClient::TNodeDescriptor& nodeDescriptor);

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

    const NNodeTrackerClient::NProto::TDiskResources& GetDiskInfo() const;

    //! Sets the node's resource usage.
    void SetResourceUsage(const TJobResources& value);

    void SetDiskInfo(const NNodeTrackerClient::NProto::TDiskResources& value);

private:
    TJobResources ResourceUsage_;
    NNodeTrackerClient::NProto::TDiskResources DiskInfo_;

    mutable NConcurrency::TReaderWriterSpinLock SpinLock_;
    TJobResources ResourceLimits_;
};

DEFINE_REFCOUNTED_TYPE(TExecNode)

////////////////////////////////////////////////////////////////////////////////

//! An immutable snapshot of TExecNode.
struct TExecNodeDescriptor
{
    TExecNodeDescriptor() = default;

    TExecNodeDescriptor(
        NNodeTrackerClient::TNodeId id,
        const TString& address,
        double ioWeight,
        const TJobResources& resourceUsage,
        const TJobResources& resourceLimits,
        const THashSet<TString>& tags);

    bool CanSchedule(const TSchedulingTagFilter& filter) const;

    NNodeTrackerClient::TNodeId Id = NNodeTrackerClient::InvalidNodeId;
    TString Address;
    double IOWeight = 0.0;
    TJobResources ResourceUsage;
    TJobResources ResourceLimits;
    THashSet<TString> Tags;

    void Persist(const TStreamPersistenceContext& context);
};

void ToProto(NScheduler::NProto::TExecNodeDescriptor* protoDescriptor, const NScheduler::TExecNodeDescriptor& descriptor);
void FromProto(NScheduler::TExecNodeDescriptor* descriptor, const NScheduler::NProto::TExecNodeDescriptor& protoDescriptor);

////////////////////////////////////////////////////////////////////////////////

//! An immutable ref-counted map of TExecNodeDescriptor-s.
struct TRefCountedExecNodeDescriptorMap
    : public TIntrinsicRefCounted
    , public TExecNodeDescriptorMap
{ };

DEFINE_REFCOUNTED_TYPE(TRefCountedExecNodeDescriptorMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
