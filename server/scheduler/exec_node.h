#pragma once

#include "public.h"
#include "scheduling_tag.h"

#include <yt/server/node_tracker_server/node.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/scheduler/scheduler_service.pb.h>
#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/lease_manager.h>
#include <yt/core/misc/property.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

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

    //! Is |true| iff heartbeat from this node is being processed at the moment.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasOngoingHeartbeat);

    //! Is |true| iff jobs are scheduled on the node at the moment by the strategy.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasOngoingJobsScheduling);

    //! Is |true| iff the node must be unregistered but it also has an ongoing
    //! heartbeat so the unregistration has to be postponed until the heartbeat processing
    //! is complete.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasPendingUnregistration);

    //! List of all jobs that should be added to jobs_to_remove
    //! in the next hearbeat response for this node.
    DEFINE_BYREF_RW_PROPERTY(std::vector<TJobId>, JobIdsToRemove)

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

    //! Returns the node's IO weight, as reported by node to master.
    double GetIOWeight() const;

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

    //! Sets the node's resource usage.
    void SetResourceUsage(const TJobResources& value);

private:
    TJobResources ResourceUsage_;

    mutable NConcurrency::TReaderWriterSpinLock SpinLock_;
    TJobResources ResourceLimits_;
    double IOWeight_ = 0;
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
        const TJobResources& resourceLimits,
        const THashSet<TString>& tags);

    bool CanSchedule(const TSchedulingTagFilter& filter) const;

    NNodeTrackerClient::TNodeId Id = NNodeTrackerClient::InvalidNodeId;
    TString Address;
    double IOWeight = 0.0;
    TJobResources ResourceLimits;
    THashSet<TString> Tags;

    void Persist(const TStreamPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

//! An immutable ref-counted list of TExecNodeDescriptor-s.
struct TExecNodeDescriptorList
    : public TIntrinsicRefCounted
{
    std::vector<TExecNodeDescriptor> Descriptors;
};

DEFINE_REFCOUNTED_TYPE(TExecNodeDescriptorList)

////////////////////////////////////////////////////////////////////////////////

//! A reduced verison of TExecNodeDescriptor, which is associated with jobs.
struct TJobNodeDescriptor
{
    TJobNodeDescriptor() = default;
    TJobNodeDescriptor(const TJobNodeDescriptor& other) = default;
    TJobNodeDescriptor(const TExecNodeDescriptor& other);

    NNodeTrackerClient::TNodeId Id = NNodeTrackerClient::InvalidNodeId;
    TString Address;
    double IOWeight = 0.0;

    void Persist(const TStreamPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
