#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/error.h>
#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(TJobId, Id);

    DEFINE_BYVAL_RW_PROPERTY(EJobType, Type);

    //! The operation the job belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TOperation*, Operation);
    
    //! Exec node where the job is running.
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);

    //! The time when the job was started.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

    DEFINE_BYREF_RW_PROPERTY(NProto::TJobResult, Result);

    //! Some rough approximation that is updated with every heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);

    //! Only valid during the heartbeat during which the job was started.
    DEFINE_BYVAL_RW_PROPERTY(NProto::TJobSpec*, Spec);

    //! Captures utilization limits suggested by the scheduler.
    /*!
     *  Receives a copy of |GetSpec()->resource_utilization()|.
     *  Never changes afterwards.
     */
    DEFINE_BYREF_RW_PROPERTY(NProto::TNodeResources, ResourceUtilization);

public:
    TJob(
        const TJobId& id,
        TOperationPtr operation,
        TExecNodePtr node,
        TInstant startTime);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
