#pragma once

#include "public.h"

#include <ytlib/scheduler/scheduler_service.pb.h>
#include <ytlib/misc/property.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Scheduler-side representation an exec-node.
class TExecNode
    : public TRefCounted
{
    //! Address as reported by master.
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Address);
    
    //! Jobs that are currently running on this node.
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TJobPtr>, Jobs);

    //! Resource limits, as reported by the node.
    DEFINE_BYREF_RW_PROPERTY(NProto::TNodeResources, ResourceLimits);

    //! The most recent resource utilization, as reported by the node.
    /*!
     *  Some fields are also updated by the scheduler strategy to
     *  reflect recent job set changes.
     *  E.g. when the scheduler decides to
     *  start a new job it decrements the appropriate counters. 
     */
    DEFINE_BYREF_RW_PROPERTY(NProto::TNodeResources, ResourceUtilization);

public:
    explicit TExecNode(const Stroka& address);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
