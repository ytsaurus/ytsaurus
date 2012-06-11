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

    //! The operation the job belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TOperation*, Operation);
    
    //! Exec node where the job is running.
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);

    //! The time when the job was started.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

    DEFINE_BYREF_RW_PROPERTY(NProto::TJobResult, Result);

    DEFINE_BYREF_RW_PROPERTY(NProto::TJobSpec, Spec);

    //! Some rough approximation that is updated with every heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);

public:
    TJob(
        const TJobId& id,
        TOperation* operation,
        TExecNodePtr node,
        const NProto::TJobSpec& spec,
        TInstant startTime);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
