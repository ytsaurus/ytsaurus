#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/lease_manager.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Scheduler-side representation an exec-node.
class TExecNode
    : public TRefCounted
{
    //! Descriptor as reported by node.
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::TNodeDescriptor, Descriptor);

    //! Jobs that are currently running on this node.
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TJobPtr>, Jobs);

    //! Resource limits, as reported by the node.
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceLimits);
    
    //! A set of scheduling tags assigned to this node.
    DEFINE_BYREF_RW_PROPERTY(yhash_set<Stroka>, SchedulingTags);

    //! The most recent resource usage, as reported by the node.
    /*!
     *  Some fields are also updated by the scheduler strategy to
     *  reflect recent job set changes.
     *  E.g. when the scheduler decides to
     *  start a new job it decrements the appropriate counters.
     */
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);

    //! Used during preemption to allow second-chance scheduling.
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsageDiscount);

    //! Controls heartbeat expiration.
    DEFINE_BYVAL_RW_PROPERTY(TLeaseManager::TLease, Lease);

    bool HasEnoughResources(const NNodeTrackerClient::NProto::TNodeResources& neededResources) const;
    bool HasSpareResources() const;

    const Stroka& GetAddress() const;

    //! Checks if the node can handle jobs demanding a certain #tag.
    bool CanSchedule(const TNullable<Stroka>& tag) const;

public:
    explicit TExecNode(const NNodeTrackerClient::TNodeDescriptor& descriptor);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
