#include "partition.h"
#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/library/min_hash_digest/min_hash_digest.h>

namespace NYT::NLsm {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

template <>
void DoRecalculatePartitionCompactionHint<EPartitionCompactionHintKind::MinHashDigest>(TPartition* partition)
{
    YT_VERIFY(!partition->IsEden());

    auto recalculationFinalizer = partition->CompactionHints().Hints()[EPartitionCompactionHintKind::MinHashDigest]
        .BuildRecalculationFinalizer(partition);

    const auto& stores = recalculationFinalizer.Stores();

    auto mountConfig = partition->GetTablet()->GetMountConfig();
    const auto& minHashDigestConfig = mountConfig->CompactionHints->MinHashDigest;

    auto similarityMode = mountConfig->MinDataVersions == 1
        ? EMinHashWriteSimilarityMode::Auxiliary
        : EMinHashWriteSimilarityMode::Primary;

    TMinHashDigestPtr cumulativeDigest;

    for (int prefixLength = 1; prefixLength <= ssize(stores); ++prefixLength) {
        auto majorTimestamp = prefixLength == ssize(stores)
            ? TInstant::Max()
            : TInstant::Seconds(UnixTimeFromTimestamp(stores[prefixLength]->GetMinTimestamp()));

        const auto& currentDigest = std::get<TMinHashDigestPtr>(
            stores[prefixLength - 1]->CompactionHints().Payloads()[EStoreCompactionHintKind::MinHashDigest]);

        cumulativeDigest = prefixLength == 1
            ? currentDigest
            : TMinHashDigest::Merge(cumulativeDigest, currentDigest);

        if (auto timestamp = cumulativeDigest->CalculateWriteDeleteSimilarityTimestamp(minHashDigestConfig->WriteDeleteSimilarity);
            timestamp && timestamp < majorTimestamp.Seconds())
        {
            recalculationFinalizer.TryApplyRecalculationByPrefix(
                TInstant::Seconds(timestamp) + mountConfig->MinDataTtl,
                EStoreCompactionReason::MinHashApplyDeletions,
                prefixLength);
        }
        if (auto timestamp = cumulativeDigest->CalculateWritesSimilarityTimestamp(minHashDigestConfig->WritesSimilarity, similarityMode);
            timestamp && timestamp < majorTimestamp.Seconds())
        {
            recalculationFinalizer.TryApplyRecalculationByPrefix(
                TInstant::Seconds(timestamp) + mountConfig->MinDataTtl,
                EStoreCompactionReason::MinHashRemoveDuplicates,
                prefixLength);
        }
    }

    if (partition->GetTablet()->GetHasAggregateColumn() || ssize(stores) > minHashDigestConfig->MaxStoreCountForExponentialCalculation) {
        return;
    }

    ui64 storeSubsetCount = 1ULL << ssize(stores);

    for (ui64 storeSubset = 1; storeSubset < storeSubsetCount; ++storeSubset) {
        cumulativeDigest = nullptr;

        for (int index = 0; index < ssize(stores); ++index) {
            if (storeSubset & (1ULL << index)) {
                const auto& currentDigest = std::get<TMinHashDigestPtr>(
                    stores[index]->CompactionHints().Payloads()[EStoreCompactionHintKind::MinHashDigest]);

                cumulativeDigest = !cumulativeDigest
                    ? currentDigest
                    : TMinHashDigest::Merge(cumulativeDigest, currentDigest);
            }
        }

        if (auto timestamp = cumulativeDigest->CalculateWritesSimilarityTimestamp(minHashDigestConfig->WritesSimilarity, similarityMode)) {
            recalculationFinalizer.TryApplyRecalculationBySubset(
                TInstant::Seconds(timestamp) + mountConfig->MinDataTtl,
                EStoreCompactionReason::MinHashRemoveDuplicates,
                storeSubset);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
