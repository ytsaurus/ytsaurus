#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/library/quantile_digest/quantile_digest.h>
#include <yt/yt/library/quantile_digest/config.h>

namespace NYT::NLsm {

using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTDigestConfigPtr GetNonCompressableDigestConfig()
{
    auto digestConfig = New<TTDigestConfig>();
    digestConfig->Delta = 0.;

    return digestConfig;
}

double GetAbsoluteRank(const IQuantileDigestPtr& digest, TInstant timestamp)
{
    return digest->GetRank(timestamp.Seconds()) * digest->GetCount();
};

const TVersionedRowDigestPtr& GetDigest(TStore* store)
{
    return std::get<TVersionedRowDigestPtr>(
        store->CompactionHints().Payloads()[EStoreCompactionHintKind::VersionedRowDigest]);
}

// NB(dave11ar): FirstTimestampDigest was added later, should keep this line
// because old chunks may lack FirstTimestampDigest.
bool HaveAllNeededDigests(TRange<TStore*> stores, const TTableMountConfigPtr& mountConfig)
{
    if (mountConfig->MinDataVersions == 0) {
        return true;
    }

    for (auto* store : stores) {
        if (!GetDigest(store)->FirstTimestampDigest) {
            return false;
        }
    }

    return true;
}

bool IsTtlCleanupExpected(
    const TVersionedRowDigestPtr& digest,
    const TTableMountConfigPtr& mountConfig,
    TInstant timestamp)
{
    i64 totalValueCount = digest->LastTimestampDigest->GetCount() + digest->AllButLastTimestampDigest->GetCount();
    double compactedValueCount = 0;

    if (mountConfig->MinDataVersions == 0) {
        auto lastDataTtl = mountConfig->MaxDataVersions == 0
            ? mountConfig->MinDataTtl
            : mountConfig->MaxDataTtl;

        compactedValueCount = GetAbsoluteRank(digest->LastTimestampDigest, timestamp - lastDataTtl) +
            GetAbsoluteRank(digest->AllButLastTimestampDigest, timestamp - mountConfig->MinDataTtl);
    } else {
        YT_VERIFY(digest->FirstTimestampDigest);

        // During compaction, all versions older than timestamp - minDataTtl will be deleted,
        // except for one version per row.
        compactedValueCount = GetAbsoluteRank(digest->LastTimestampDigest, timestamp - mountConfig->MinDataTtl) +
            GetAbsoluteRank(digest->AllButLastTimestampDigest, timestamp - mountConfig->MinDataTtl) -
            GetAbsoluteRank(digest->FirstTimestampDigest, timestamp - mountConfig->MinDataTtl);
    }

    return compactedValueCount / totalValueCount > mountConfig->CompactionHints->RowDigest->MaxObsoleteTimestampRatio;
}

TInstant CalculateTtlCleanupExpected(
    const TVersionedRowDigestPtr& digest,
    const TTableMountConfigPtr& mountConfig,
    TInstant minStoresTimestamp,
    TInstant maxStoresTimestamp,
    TInstant majorTimestamp = TInstant::Max())
{
    auto leftTimestampBound = minStoresTimestamp + mountConfig->MinDataTtl;
    auto rightTimestampBound = std::min(
        maxStoresTimestamp + mountConfig->MaxDataTtl,
        majorTimestamp - TDuration::MicroSeconds(1));

    if (leftTimestampBound > rightTimestampBound) {
        return {};
    }

    static constexpr auto CompactionTimestampAccuracy = TDuration::Seconds(1);
    while (rightTimestampBound - leftTimestampBound > CompactionTimestampAccuracy) {
        auto midTimestamp = leftTimestampBound + (rightTimestampBound - leftTimestampBound) / 2;

        if (IsTtlCleanupExpected(digest, mountConfig, midTimestamp)) {
            rightTimestampBound = midTimestamp;
        } else {
            leftTimestampBound = midTimestamp;
        }
    }

    if (IsTtlCleanupExpected(digest, mountConfig, rightTimestampBound)) {
        return rightTimestampBound;
    }

    return {};
}

TInstant CalculateTooManyTimestamps(
    const TVersionedRowDigestPtr& digest,
    const TTableMountConfigPtr& mountConfig,
    TInstant majorTimestamp = TInstant::Max())
{
    ui32 timestampIndex = std::countr_zero<ui32>(mountConfig->CompactionHints->RowDigest->MaxTimestampsPerValue);

    // TODO(dave11ar): YT-27427.
    if (timestampIndex < ssize(digest->EarliestNthTimestamp)) {
        auto compactionTimestamp = TInstant::Seconds(digest->EarliestNthTimestamp[timestampIndex]) + mountConfig->MinDataTtl;

        if (compactionTimestamp < majorTimestamp) {
            return compactionTimestamp;
        }
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

template <>
void DoRecalculateStoreCompactionHint<EStoreCompactionHintKind::VersionedRowDigest>(TStore* store)
{
    auto recalculationFinalizer = store->CompactionHints().Hints()[EStoreCompactionHintKind::VersionedRowDigest]
        .BuildRecalculationFinalizer(store);

    auto mountConfig = store->GetTablet()->GetMountConfig();

    const auto& digest = GetDigest(store);

    auto minStoreTimestamp = TimestampToInstant(store->GetMinTimestamp()).first;
    auto maxStoreTimestamp = TimestampToInstant(store->GetMaxTimestamp()).second;

    if (HaveAllNeededDigests({store}, mountConfig)) {
        if (auto timestamp = CalculateTtlCleanupExpected(digest, mountConfig, minStoreTimestamp, maxStoreTimestamp)) {
            recalculationFinalizer.TryApplyRecalculation(timestamp, EStoreCompactionReason::TtlCleanupExpected);
        }
    }
    if (auto timestamp = CalculateTooManyTimestamps(digest, mountConfig)) {
        recalculationFinalizer.TryApplyRecalculation(timestamp, EStoreCompactionReason::TooManyTimestamps);
    }
}

template <>
void DoRecalculatePartitionCompactionHint<EPartitionCompactionHintKind::AggregateVersionedRowDigest>(TPartition* partition)
{
    YT_VERIFY(!partition->IsEden());

    auto recalculationFinalizer = partition->CompactionHints().Hints()[EPartitionCompactionHintKind::AggregateVersionedRowDigest]
        .BuildRecalculationFinalizer(partition);

    auto mountConfig = partition->GetTablet()->GetMountConfig();

    const auto& stores = recalculationFinalizer.Stores();
    if (stores.empty() || ssize(stores) > mountConfig->CompactionHints->RowDigest->MaxStoreCount) {
        return;
    }

    bool haveAllNeededDigests = HaveAllNeededDigests(stores, mountConfig);

    auto minStoresTimestamp = TimestampToInstant(stores.front()->GetMinTimestamp()).first;
    auto maxStoresTimestamp = TInstant::Zero();

    static auto NonCompressableDigestConfig = GetNonCompressableDigestConfig();
    auto cumulativeDigest = New<TVersionedRowDigest>(NonCompressableDigestConfig);

    for (int prefixLength = 1; prefixLength <= ssize(stores); ++prefixLength) {
        auto* store = stores[prefixLength - 1];
        maxStoresTimestamp = std::max(maxStoresTimestamp, TimestampToInstant(store->GetMaxTimestamp()).second);

        auto majorTimestamp = prefixLength < ssize(stores)
            ? TimestampToInstant(stores[prefixLength]->GetMinTimestamp()).first
            : TInstant::Max();

        cumulativeDigest->MergeWith(GetDigest(store));

        if (haveAllNeededDigests) {
            if (auto timestamp = CalculateTtlCleanupExpected(cumulativeDigest, mountConfig, minStoresTimestamp, maxStoresTimestamp, majorTimestamp)) {
                recalculationFinalizer.TryApplyRecalculationByPrefix(timestamp, EStoreCompactionReason::AggregateTtlCleanupExpected, prefixLength);
            }
        }
        if (auto timestamp = CalculateTooManyTimestamps(cumulativeDigest, mountConfig, majorTimestamp)) {
            recalculationFinalizer.TryApplyRecalculationByPrefix(timestamp, EStoreCompactionReason::AggregateDeleteTooManyTimestamps, prefixLength);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
