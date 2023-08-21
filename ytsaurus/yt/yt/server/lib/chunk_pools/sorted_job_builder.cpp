#include "sorted_job_builder.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

void TSortedJobOptions::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, EnableKeyGuarantee);
    Persist(context, PrimaryPrefixLength);
    Persist(context, ForeignPrefixLength);
    Persist(context, PrimaryComparator);
    Persist(context, ForeignComparator);
    Persist(context, MaxTotalSliceCount);
    Persist(context, EnablePeriodicYielder);
    Persist(context, ConsiderOnlyPrimarySize);
    Persist(context, PivotKeys);
    Persist(context, ShouldSlicePrimaryTableByKeys);
    Persist(context, ValidateOrder);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
