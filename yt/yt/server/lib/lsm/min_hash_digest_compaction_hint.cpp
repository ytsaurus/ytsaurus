#include "partition.h"

#include <yt/yt/library/min_hash_digest/min_hash_digest.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

template <>
void DoRecalculatePartitionCompactionHint<EPartitionCompactionHintKind::MinHashDigest>(TPartition* partition)
{
    YT_VERIFY(!partition->IsEden());

    std::vector<TMinHashDigestPtr> minHashDigests;
    minHashDigests.reserve(partition->Stores().size());
    for (const auto& store : partition->Stores()) {
        minHashDigests.push_back(std::get<TStoreCompactionHint::TMinHashDigestPayload>(
            store->CompactionHints().Payloads()[EStoreCompactionHintKind::MinHashDigest]));
    }

    auto recalculationFinalizer = partition->CompactionHints().Hints()[EPartitionCompactionHintKind::MinHashDigest]
        .BuildRecalculationFinalizer(partition);
    // TODO(dave11ar): Add logic.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
