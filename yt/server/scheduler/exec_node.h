#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

#include <ytlib/chunk_client/node_directory.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Scheduler-side representation an exec-node.
class TExecNode
    : public TRefCounted
{
    //! Descriptor as reported by node.
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TNodeDescriptor, Descriptor);

    //! Jobs that are currently running on this node.
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TJobPtr>, Jobs);

    //! Resource limits, as reported by the node.
    DEFINE_BYREF_RW_PROPERTY(NProto::TNodeResources, ResourceLimits);

    //! The most recent resource usage, as reported by the node.
    /*!
     *  Some fields are also updated by the scheduler strategy to
     *  reflect recent job set changes.
     *  E.g. when the scheduler decides to
     *  start a new job it decrements the appropriate counters.
     */
    DEFINE_BYREF_RW_PROPERTY(NProto::TNodeResources, ResourceUsage);

    //! Used during preemption to allow second-chance scheduling.
    DEFINE_BYREF_RW_PROPERTY(NProto::TNodeResources, ResourceUsageDiscount);

    bool HasEnoughResources(const NProto::TNodeResources& neededResources) const;
    bool HasSpareResources() const;

public:
    explicit TExecNode(const NChunkClient::TNodeDescriptor& descriptor);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
