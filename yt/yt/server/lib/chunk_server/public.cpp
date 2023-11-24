#include "public.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

NScheduler::TJobId ToSchedulerJobId(TJobId jobId)
{
    return NScheduler::TJobId(jobId.Underlying());
}

TJobId FromSchedulerJobId(NScheduler::TJobId jobId)
{
    return TJobId(jobId.Underlying());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
