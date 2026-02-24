#include "partition.h"

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

template <>
void DoRecalculatePartitionCompactionHint<EPartitionCompactionHintKind::AggregateVersionedRowDigest>(TPartition* partition)
{
    YT_VERIFY(!partition->IsEden());

    auto recalculationFinalizer = partition->CompactionHints().Hints()[
        EPartitionCompactionHintKind::AggregateVersionedRowDigest].BuildRecalculationFinalizer(partition);
    // TODO(dave11ar): Add logic.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm

