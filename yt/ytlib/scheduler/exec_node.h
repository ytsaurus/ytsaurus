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

    //! The most recent utilization as reported by the node.
    /*!
     *  Some fields are also updated by the scheduler strategy to
     *  reflect recent job set changes. E.g. when the scheduler decided to
     *  start a new job it decrements |free_slot_count|. 
     */
    DEFINE_BYREF_RW_PROPERTY(NProto::TNodeUtilization, Utilization);

public:
    TExecNode(const Stroka& address);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
