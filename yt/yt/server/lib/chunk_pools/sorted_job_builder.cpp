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
    Persist(context, PivotKeys);
    Persist(context, LogDetails);
    Persist(context, ShouldSlicePrimaryTableByKeys);
    Persist(context, SortByPosition);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
