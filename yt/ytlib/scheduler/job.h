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

    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);

    //! The operation the job belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TOperation*, Operation);
    
    //! Exec node where the job is running.
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);

    //! The time when the job was started.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

    DEFINE_BYREF_RW_PROPERTY(NProto::TJobResult, Result);

    //! A spec to be passed to the exec node.
    /*!
     *  Since the spec might be relatively heavy,
     *  it is cleared immediately after constructing the scheduling
     *  request.
     */
    DEFINE_BYREF_RW_PROPERTY(NProto::TJobSpec, Spec);

    //! Some rough approximation that is updated with every heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);

public:
    TJob(
        const TJobId& id,
        EJobType type,
        TOperation* operation,
        TExecNodePtr node,
        const NProto::TJobSpec& spec,
        TInstant startTime);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
