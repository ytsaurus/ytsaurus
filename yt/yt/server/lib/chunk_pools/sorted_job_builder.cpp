#include "sorted_job_builder.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

void TSortedJobOptions::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, EnableKeyGuarantee);
    PHOENIX_REGISTER_FIELD(2, PrimaryPrefixLength);
    PHOENIX_REGISTER_FIELD(3, ForeignPrefixLength);
    PHOENIX_REGISTER_FIELD(4, PrimaryComparator);
    PHOENIX_REGISTER_FIELD(5, ForeignComparator);
    PHOENIX_REGISTER_FIELD(6, MaxTotalSliceCount);
    PHOENIX_REGISTER_FIELD(7, EnablePeriodicYielder);
    PHOENIX_REGISTER_FIELD(8, ConsiderOnlyPrimarySize);
    PHOENIX_REGISTER_FIELD(9, PivotKeys);
    PHOENIX_REGISTER_FIELD(10, ShouldSlicePrimaryTableByKeys);
    PHOENIX_REGISTER_FIELD(11, ValidateOrder);
}

PHOENIX_DEFINE_TYPE(TSortedJobOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
