#include "public.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TAllocationId AllocationIdFromJobId(TJobId jobId)
{
    auto allocationIdGuid = jobId.Underlying();
    allocationIdGuid.Parts32[0] &= (1 << 24) - 1;
    return TAllocationId(allocationIdGuid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
