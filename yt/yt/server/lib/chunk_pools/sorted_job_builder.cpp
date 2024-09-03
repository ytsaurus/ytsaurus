#include "sorted_job_builder.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

void TSortedJobOptions::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::EnableKeyGuarantee>("enable_key_guarantee")();
    registrar.template Field<2, &TThis::PrimaryPrefixLength>("primary_prefix_length")();
    registrar.template Field<3, &TThis::ForeignPrefixLength>("foreign_prefix_length")();
    registrar.template Field<4, &TThis::PrimaryComparator>("primary_comparator")();
    registrar.template Field<5, &TThis::ForeignComparator>("foreign_comparator")();
    registrar.template Field<6, &TThis::MaxTotalSliceCount>("max_total_slice_count")();
    registrar.template Field<7, &TThis::EnablePeriodicYielder>("enable_periodic_yielder")();
    registrar.template Field<8, &TThis::ConsiderOnlyPrimarySize>("consider_only_primary_size")();
    registrar.template Field<9, &TThis::PivotKeys>("pivot_keys")();
    registrar.template Field<10, &TThis::ShouldSlicePrimaryTableByKeys>("should_slice_primary_table_by_keys")();
    registrar.template Field<11, &TThis::ValidateOrder>("validate_order")();
}

PHOENIX_DEFINE_TYPE(TSortedJobOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
