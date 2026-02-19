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

    auto& hint = partition->CompactionHints().Hints()[EPartitionCompactionHintKind::MinHashDigest];

    // TODO(dave11ar): Add logic.
    hint.MakeDecision(TInstant::Zero(), EStoreCompactionReason::None);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
