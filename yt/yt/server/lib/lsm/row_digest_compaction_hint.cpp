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
    TInstant majorTimestamp)
{
    // NB(dave11ar): FirstTimestampDigest was added later, should keep this line
    // because old chunks may lack FirstTimestampDigest.
    if (!digest->FirstTimestampDigest && mountConfig->MinDataVersions == 1) {
        return {};
    }

    auto leftTimestampBound = TInstant::Seconds(std::min(
        digest->AllButLastTimestampDigest->GetQuantile(0),
        digest->LastTimestampDigest->GetQuantile(0)));

    auto rightTimestampBound = std::max(
        TInstant::Seconds(std::max(
            digest->AllButLastTimestampDigest->GetQuantile(1),
            digest->LastTimestampDigest->GetQuantile(1))) + mountConfig->MaxDataTtl,
        majorTimestamp - TDuration::MicroSeconds(1));

    constexpr auto CompactionTimestampAccuracy = TDuration::Seconds(1);
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
    TInstant majorTimestamp)
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
        .BuildRecalculationFinalizer();

    auto majorTimestamp = TInstant::Max();
    const auto& mountConfig = store->GetTablet()->GetMountConfig();
    const auto& digest = std::get<TVersionedRowDigestPtr>(
        store->CompactionHints().Payloads()[EStoreCompactionHintKind::VersionedRowDigest]);

    if (auto timestamp = CalculateTtlCleanupExpected(digest, mountConfig, majorTimestamp)) {
        recalculationFinalizer.TryApplyRecalculation(timestamp, EStoreCompactionReason::TtlCleanupExpected);
    }
    if (auto timestamp = CalculateTooManyTimestamps(digest, mountConfig, majorTimestamp)) {
        recalculationFinalizer.TryApplyRecalculation(timestamp, EStoreCompactionReason::TooManyTimestamps);
    }
}

template <>
void DoRecalculatePartitionCompactionHint<EPartitionCompactionHintKind::AggregateVersionedRowDigest>(TPartition* partition)
{
    YT_VERIFY(!partition->IsEden());

    auto recalculationFinalizer = partition->CompactionHints().Hints()[EPartitionCompactionHintKind::AggregateVersionedRowDigest]
        .BuildRecalculationFinalizer(partition);

    const auto& mountConfig = partition->GetTablet()->GetMountConfig();
    const auto& rowDigestConfig = mountConfig->CompactionHints->RowDigest;

    const auto& stores = recalculationFinalizer.Stores();

    if (ssize(stores) > rowDigestConfig->MaxStoreCount) {
        return;
    }

    static auto NonCompressableDigestConfig = GetNonCompressableDigestConfig();
    auto cumulativeDigest = New<TVersionedRowDigest>(NonCompressableDigestConfig);

    for (int prefixLength = 1; prefixLength <= ssize(stores); ++prefixLength) {
        auto majorTimestamp = prefixLength == ssize(stores)
            ? TInstant::Max()
            : TInstant::Seconds(UnixTimeFromTimestamp(stores[prefixLength]->GetMinTimestamp()));

        cumulativeDigest->MergeWith(std::get<TVersionedRowDigestPtr>(
            stores[prefixLength - 1]->CompactionHints().Payloads()[EStoreCompactionHintKind::VersionedRowDigest]));

        if (auto timestamp = CalculateTtlCleanupExpected(cumulativeDigest, mountConfig, majorTimestamp)) {
            recalculationFinalizer.TryApplyRecalculationByPrefix(timestamp, EStoreCompactionReason::AggregateTtlCleanupExpected, prefixLength);
        }
        if (auto timestamp = CalculateTooManyTimestamps(cumulativeDigest, mountConfig, majorTimestamp)) {
            recalculationFinalizer.TryApplyRecalculationByPrefix(timestamp, EStoreCompactionReason::AggregateTtlCleanupExpected, prefixLength);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
