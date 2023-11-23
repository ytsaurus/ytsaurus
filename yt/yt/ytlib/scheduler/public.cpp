#include "public.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobId JobIdFromAllocationId(TAllocationId allocationId)
{
    return TJobId(allocationId.Underlying());
}

TAllocationId AllocationIdFromJobId(TJobId jobId)
{
    return TAllocationId(jobId.Underlying());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
