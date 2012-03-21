#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
{
    //! Job id.
    DEFINE_BYVAL_RO_PROPERTY(TJobId, JobId);

    //! Operation the job belongs to. Weak ref breaks the cycle.
    DEFINE_BYVAL_RO_PROPERTY(TWeakPtr<TOperation>, Operation);
    
    //! Exec node where the job is running.
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);

    //! The time when the job was started.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

public:
    TJob(
        const TJobId& jobId,
        TOperationPtr operation,
        TExecNodePtr node,
        TInstant startTime);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
